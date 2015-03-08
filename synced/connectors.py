import abc
import re
from collections import OrderedDict
import uuid
from datetime import datetime

import time_uuid
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from pyes import ES
from pyes.query import *
from pyes.managers import Indices
from dateutil.parser import parse


class Connector(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def normalize(self, data):
        """Normalize data, if needed."""
        return

    @abc.abstractmethod
    def convert(self, data):
        """Convert data to be persisted, if needed."""
        return

    @abc.abstractmethod
    def list_tables(self):
        """List all available tables."""
        return

    @abc.abstractmethod
    def load(self, table, begin, end):
        """Retrieve data from the input source and return it."""
        return

    @abc.abstractmethod
    def upsert_if_newer(self, table, data):
        """Update the data object if an older version in persisted or insert if it do not exists."""
        return

    @abc.abstractmethod
    def close(self):
        """Close database connection."""
        return


class CassandraConnector(Connector):
    def __init__(self, keyspace):
        self.keyspace = keyspace
        self.cluster = Cluster()
        self.session = self.cluster.connect(keyspace=keyspace)
        self.session.row_factory = dict_factory

    def normalize(self, data):
        def normalize_entry(key, value):
            if key == u"id" and isinstance(value, uuid.UUID):
                return str(value)
            elif key == u"tmstmp":
                if not isinstance(value, time_uuid.TimeUUID):
                    return time_uuid.TimeUUID.convert(value, randomize=False, lowest_val=True).get_datetime()
                else:
                    return value.get_datetime()
            else:
                return value

        return {key: normalize_entry(key, value) for key, value in data.iteritems()}

    def convert(self, data):
        def convert_entry(key, value):
            if key == "id":
                return value if isinstance(value, uuid.UUID) else uuid.UUID(value)
            elif key == "tmstmp" and isinstance(value, datetime):
                return uuid.UUID(str(time_uuid.TimeUUID.convert(value, randomize=False, lowest_val=True)))
            else:
                return value

        return OrderedDict({key: convert_entry(key, value) for key, value in data.iteritems()})

    def list_tables(self):
        return [name for name in self.cluster.metadata.keyspaces[self.keyspace].tables];

    def load(self, table, begin, end):
        prepared_stmt = self.session.prepare("SELECT * FROM " + table +
                                             " WHERE tmstmp > minTimeuuid(?) AND tmstmp <= minTimeuuid(?)" +
                                             " ALLOW FILTERING;")
        bound_stmt = prepared_stmt.bind([begin, end])
        for entry in self.session.execute(bound_stmt):
            yield self.normalize(entry)

    def upsert_if_newer(self, table, data):
        _data = self.convert(data)
        prepared_stmt = self.session.prepare("INSERT INTO " + table + " (" +
                                             ", ".join(key for key in _data) +
                                             ") VALUES (" +
                                             ", ".join("?" for n in xrange(len(_data))) +
                                             ")")
        bound_stmt = prepared_stmt.bind(_data.values())
        return self.session.execute(bound_stmt)

    def close(self):
        self.session.shutdown()
        self.cluster.shutdown()


class ESConnector(Connector):
    def __init__(self, index_name):
        self.client = ES("localhost:9200")
        self.indexName = index_name

    def normalize(self, data):
        def normalize_value(value):
            if isinstance(value, six.integer_types):
                return value
            elif isinstance(value, six.string_types) or isinstance(value, six.text_type):
                try:
                    return parse(value) if re.match("\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d.*", value) else value
                except:
                    return value
            else:
                return str(value)

        return {key: normalize_value(value) for (key, value) in data.iteritems()}

    def convert(self, data):
        def convert_value(value):
            if isinstance(value, six.integer_types):
                return str(value)
            elif isinstance(value, six.string_types) or isinstance(value, six.text_type):
                return '"' + value + '"'
            elif isinstance(value, datetime):
                return '"' + value.isoformat() + '"'
            else:
                return str(value)

        return OrderedDict({key: convert_value(value) for (key, value) in data.iteritems()})

    def list_tables(self):
        return [k for (k, v) in Indices(self.client).get_mapping(indices=self.indexName).indices[self.indexName]]

    def load(self, table, begin, end):
        string_query = 'tmstmp:["' + begin.isoformat() + '" TO "' + end.isoformat() + '"]'
        for entry in self.client.search(query=QueryStringQuery(string_query), indices=self.indexName, doc_types=table):
            yield self.normalize(entry)

    def upsert_if_newer(self, table, data):
        _data = self.convert(data)
        script_data = ";".join("ctx._source." + key + "=" + value
                               for key, value in _data.iteritems()) + ";"
        script = "if(ctx._source.tmstmp>="+_data["tmstmp"]+"){ctx.op=\"noop\";}else{"+script_data+"}"
        return self.client.update(
            index=self.indexName,
            doc_type=table,
            id=data["id"],
            script=script,
            upsert=data,
            lang="groovy")

    def close(self):
        pass
