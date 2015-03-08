import sys
import getopt
from os import path
from datetime import datetime
import connectors
import logging
import logging.config


logging.config.fileConfig('logging.ini')
logger = logging.getLogger(__name__)


def sync(begin, end, keyspace, index_name):
    cassandra_conn = connectors.CassandraConnector(keyspace)
    es_conn = connectors.ESConnector(index_name)

    try:
        sync_db(cassandra_conn, es_conn, begin, end)
        sync_db(es_conn, cassandra_conn, begin, end)
    finally:
        cassandra_conn.close()
        es_conn.close()


def sync_db(source, target, begin, end):
    for table in source.list_tables():
        for entry in source.load(table, begin, end):
            target.upsert_if_newer(table, entry)


def load_params(argv):
    begin = end = -1
    keyspace = indexname = ""
    try:
        opts, args = getopt.getopt(argv, "hbek:i:", ["begin=", "end=", "keyspace", "indexname"])
    except getopt.GetoptError:
        print "synced -b <begin> -e <end> -k <keyspace> -i <indexname>"
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print "synced -b <begin> -e <end> -k <keyspace> -i <indexname>"
            sys.exit()
        elif opt in ("-b", "--begin"):
            begin = float(arg)
        elif opt in ("-e", "--end"):
            end = float(arg)
        elif opt in ("-k", "--keyspace"):
            keyspace = arg
        elif opt in ("-i", "--indexname"):
            indexname = arg

    if "begin" == -1:
        begin_datetime = datetime.utcfromtimestamp(begin)
    else:
        if path.exists("last"):
            with open("last") as f:
                begin_datetime = datetime.utcfromtimestamp(float(f.read()))
        else:
            begin_datetime = datetime.utcfromtimestamp(0)

    if "end" == -1:
        end_datetime = datetime.utcfromtimestamp(end)
    else:
        end_datetime = datetime.utcnow()

    return begin_datetime, end_datetime, keyspace, indexname


def run():
    logger.info("Starting sync process..")
    try:
        logger.debug("(1) Load params...")
        begin, end, keyspace, indexname = load_params(sys.argv[1:])
        logger.debug("(1.1) begin: %s", begin)
        logger.debug("(1.2) end: %s", end)
        logger.debug("(1.3) keyspace: %s", keyspace)
        logger.debug("(1.4) indexname: %s", indexname)
        logger.debug("(2) Syncing...")
        sync(begin, end, keyspace, indexname)
        logger.debug("(3) Saving end interval...")
        with open("last", "w") as f:
            f.write(str((end - datetime.utcfromtimestamp(0)).total_seconds()))
        logger.info("Databases synchronized.")
        return 0
    except Exception, e:
        logging.exception(e)
        return 1


if __name__ == "__main__":
    sys.exit(run())