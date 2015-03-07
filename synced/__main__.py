#!/usr/bin/python

import sys
import getopt
from os import path
from datetime import datetime
import synced


def load_params(argv):
    begin = end = -1
    keyspace = indexname = ''
    try:
        opts, args = getopt.getopt(argv, "hbek:i:", ["begin=", "end=", "keyspace", "indexname"])
    except getopt.GetoptError:
        print 'synced -b <begin> -e <end> -k <keyspace> -i <indexname>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'synced -b <begin> -e <end> -k <keyspace> -i <indexname>'
            sys.exit()
        elif opt in ("-b", "--begin"):
            begin = float(arg)
        elif opt in ("-e", "--end"):
            end = float(arg)
        elif opt in ("-k", "--keyspace"):
            keyspace = arg
        elif opt in ("-i", "--indexname"):
            indexname = arg

    if 'begin' == -1:
        begin_datetime = datetime.utcfromtimestamp(begin)
    else:
        if path.exists("last"):
            with open("last") as f:
                begin_datetime = datetime.utcfromtimestamp(float(f.read()))
        else:
            begin_datetime = datetime.utcfromtimestamp(0)

    if 'end' == -1:
        end_datetime = datetime.utcfromtimestamp(end)
    else:
        end_datetime = datetime.utcnow()

    return begin_datetime, end_datetime, keyspace, indexname


def main():
    print 'Synced starting...'
    print '(1) Load params...'
    begin, end, keyspace, indexname = load_params(sys.argv[1:])
    print 'begin:', begin, '\nend:', end, '\nkeyspace:', keyspace, '\nindexname:', indexname
    print 'Done.\n(2) Syncing...'
    synced.sync(begin, end, keyspace, indexname)
    print 'Done.\n(3) Saving end interval...'
    with open("last", "w") as f:
        f.write(str((end - datetime.utcfromtimestamp(0)).total_seconds()))
    print 'Done.\nBye bye!'


if __name__ == "__main__":
    main()