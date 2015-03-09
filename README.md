# synced
Database synchronization based on timestamp conditional upserts.

**synced** is based on upserts but only if a newer version is available.  As it is a atomic operation, we don't have problems with concurrent writes or race conditions.

This solution requires the following inputs:

* Range (begin, end): It is used to define our sync range in both databases.  This information isn't required.  In the first execution, the default range is between the beginning of the unix timestamp (1970-01-01) and the moment the application started running.  In the end of each sync, we persist the end of the range, to be used in the next execution.  We recommend to specify the range only if you want a given interval.
* Keyspace: It is used to define which Cassandra keyspace we will sync.  It is required.
* Index name: It is used to define which Elasticseach index we will sync.  It is required.

And the tables?  Which one?  Simple, we sync all tables in a keyspace with all types in a index.  Each Cassandra's table will be represented as a type in a Elasticsearch's index, and vice versa.

The pseudo-algorithm is presented below.

```
begin, end, keyspace, indexname = load_input_data()

cassandra_conn = create_cassandra_connection(input.keyspace)
es_conn = create_es_connection(input.indexname)

sync(cassandra_conn, es_conn, begin, end)
sync(es_conn, cassandra_conn, begin, end)

cassandra_conn.close()
es_conn.close()

persist_end_for_next_execution(end)

def sync(source, target, begin, end):
  for table in source.tables:
    for entry in source.load_entries(table, begin, end):
      target.upsert_if_newer(entry)
```

## Why this solution works?

This solution is based on upserts, that only occurs if the given timestamp is newer that the persisted version.  This operation is a atomic operation, so concurrent access isn't a problem.  Also, we sync a well-defined range, preventing that new entries interfere in the current execution.  

If a update don't change a document that is selected to be synced, it will be treated in the next execution because of the well-defined range.  If an update change an entry that will be synced in a given execution, you have two scenarios:

1. Updated entry will be synced.  No problem, it will work as expected, but will sync a newer version.
2. Updated entry was already synced.  In the next execution this entry will be synced because it is a newer version.

Given this restriction, it doesn't matter which database is synced first. At the end of the synchronization, the newer versions of each database will be persisted.

## Do I need some specific structure (table/index/type) to work properly?

Yes.  Each table/type need the given fields:

* An identifier that is a UUID type 4. Its name must be **id**.  In Cassandra, it is a **uuid**.  In ES, it is a **string**.
* A timestamp to define its last change in the entry/document.  Its name must be **tmstmp**.  In Cassandra, it is a **time_uuid**.  In ES, it is a **date** with format **dateOptionalTime**.

Also, each Cassandra table must have **id** and **tmstmp** as **primary key**.  Each update in these tables actually must be a insert with same id and the function **now()** as **tmstmp**.  At ES, each insert or update must set the **tmstmp**.

## How to run

Just checkout the code and run the following command for help:

```
./sync.py -h
```

You can define the range in milliseconds, the keyname and index name.  Examples are presented below.

```
## Sync everything in January 2015
synced -b 1420070400 -e 1422748800 -k app -i app

## Sync from the last execution to now
synced -k app -i app
```

We use the file called *last* to persist the end of the range of the last execution.  Feel free to edit or remove this information.  We will improve this solution, as detailed in *Limitations / Future work* section.

## User cases

### Running as daemon with regular intervals (scheduler)

Just use crontab :)  In this case, use the [sync.py](https://github.com/robertowm/synced/blob/master/sync.py) to configure the crontab properly. [Need help? Click here.](https://help.ubuntu.com/community/CronHowto)

Question:  But I use Windows...  Can you help me?

Answer:  [Sure.  One of many solutions.](http://www.ubuntu.com/download)

Question:  But I really need a "daemon" solution with a scheduler in Python.  Can you?

Answer:  I'm thinking in doing something like that for learning purpose.  In the meanwhile, you can use this [great article](http://simeonfranklin.com/blog/2012/aug/14/scheduling-tasks-python/) as a workaround.

## Limitations / Future work

| Limitations | Future works |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| Databases configurations are hard-coded. | Parametrize it. |
| Sync all tables, but not a given set of tables. | Parametrize it, to allow to sync modes: all tables; given set of tables. |
| At the end of each execution, the end of the range are persisted in a file, called "last".  If another execution occurs with different parameters, by default it will use the last end as the beginning of the execution.  Unfortunately, it is wrong. | Develop a smarter persistence system, that allow multiple configurations. |
