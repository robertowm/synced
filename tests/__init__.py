from synced.connectors import CassandraConnector, ESConnector
from datetime import datetime
import time_uuid


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

def testES():
    conn = ESConnector('demo')
    date = time_uuid.TimeUUID.convert(datetime(2015, 2, 1))
    conn.upsert('users', {'name': 'Mari...', 'id': 2, 'tmstmp': date})
    for row in conn.load(datetime.utcfromtimestamp(0), datetime(2020, 1, 1)):
        print '\t', row
    conn.close()

testES()