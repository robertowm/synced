from synced.connectors import CassandraConnector
from datetime import datetime


##refact tests

def testCassandra():
    conn = CassandraConnector('demo')
    for table in conn.list_tables():
        print 'Table:', table
        for row in conn.load(table, datetime.utcfromtimestamp(0), datetime(2020, 1, 1)):
            print '\t', row
            row['name'] += '.'
            print '\t', row
            conn.upsert(table, row)
        print '---'
        for row in conn.load(table, datetime.utcfromtimestamp(0), datetime(2020, 1, 1)):
            print '\t', row
    conn.close()

testCassandra()