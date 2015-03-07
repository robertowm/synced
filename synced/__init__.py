from connectors import Connector, CassandraConnector, ESConnector


def sync(begin, end, keyspace, index_name):
    cassandra_conn = CassandraConnector(keyspace)
    es_conn = ESConnector(index_name)

    try:
        sync_db(cassandra_conn, es_conn, begin, end)
        sync_db(es_conn, cassandra_conn, begin, end)
    finally:
        cassandra_conn.close()
        es_conn.close()


def sync_db(source, target, begin, end):
    print 'Source: ' + str(type(source))
    print 'Target: ' + str(type(target))
    for table in source.list_tables():
        for entry in source.load(table, begin, end):
            print 'Entry: ' + str(entry)
            response = target.upsert_if_newer(table, entry)
            print response


if __name__ == '__main__':
    print 'running...'