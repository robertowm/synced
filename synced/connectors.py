import abc
import json, copy, six
from types import *
import uuid, time_uuid
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from pyes import ES
from pyes.query import *
from pyes.managers import Indices


class Connector(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def list_tables(self):
        """List all available tables."""
        return

    @abc.abstractmethod
    def load(self, table, begin, end):
        """Retrieve data from the input source and return it."""
        return

    @abc.abstractmethod
    def upsert(self, table, data):
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

    def list_tables(self):
        return [name for name in self.cluster.metadata.keyspaces[self.keyspace].tables];

    def load(self, table, begin, end):
        prepared_stmt = self.session.prepare("SELECT * FROM " + table +
                                             " WHERE tmstmp > minTimeuuid(?) AND tmstmp <= minTimeuuid(?)" +
                                             " ALLOW FILTERING;")
        bound_stmt = prepared_stmt.bind([begin, end])
        return self.session.execute(bound_stmt)

    def upsert(self, table, data):
        prepared_stmt = self.session.prepare("INSERT INTO " + table + " (" +
                                             ", ".join(key for key in data) +
                                             ") VALUES (" +
                                             ", ".join("?" for n in xrange(len(data))) +
                                             ")")
        bound_stmt = prepared_stmt.bind([value for value in data.values()])
        return self.session.execute(bound_stmt)

    def close(self):
        self.cluster.shutdown()
        self.session.shutdown()


class ESConnector:
    def __init__(self, index_name):
        self.client = ES('localhost:9200')
        self.indexName = index_name

    def serialize(self, value):
        if type(value) is dict:
            _value = {}
            for key in value:
                _value[key] = self.serialize(value[key])
            return _value
        elif isinstance(value, six.integer_types):
            return str(value)
        elif isinstance(value, six.string_types) or isinstance(value, six.text_type):
            return '"' + value + '"'
        elif isinstance(value, uuid.UUID):
            return '"' + time_uuid.TimeUUID.convert(value).get_datetime().isoformat() + '"'
        elif isinstance(value, time_uuid.TimeUUID):
            return '"' + value.get_datetime().isoformat() + '"'
        elif isinstance(value, datetime):
            return '"' + value.isoformat() + '"'
        else:
            return str(value)

    def list_tables(self):
        return [k for (k, v) in Indices(self.client).get_mapping(indices=self.indexName).indices[self.indexName]]

    def load(self, table, begin, end):
        string_query = "tmstmp: [" + self.serialize(begin) + " TO " + self.serialize(end) + "]"
        return self.client.search(query=QueryStringQuery(string_query), indices=self.indexName, doc_types=table)

    def upsert(self, table, data):
        _data = self.serialize(data)
        script_data = ";".join( "ctx._source." + key + "=" + value
                        for key, value in _data.iteritems()) + ";"
        script = "if(ctx._source.tmstmp>="+_data['tmstmp']+"){ctx.op=\"noop\";}else{"+script_data+"}"
        params = upsert = _data
        return self.client.update(
            index=self.indexName,
            doc_type=table,
            id=_data['id'],
            script=script,
            params=params,
            upsert=upsert,
            lang='groovy')

    def close(self):
        pass
