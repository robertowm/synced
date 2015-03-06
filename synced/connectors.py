import abc
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from elasticsearch import Elasticsearch


class Connector(object):
    __metaclass__ = abc.ABCMeta

    def list_tables(self):
        """List tables name."""
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

    def session(self):
        return self.session

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


class ESConnector(Connector):

    def __init__(self):
        self.client = Elasticsearch()

    def list_tables(self):
        pass

    def load(self, table, begin, end):
        pass

    def upsert(self, table, data):
        pass
