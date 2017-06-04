sas2mongo
=========

SAS file to MongoDB converter

Download: [sas2mongo.jar] (https://github.com/xantorohara/sas2mongo/raw/master/target/sas2mongo.jar)

Usage: java -jar sas2mongo.jar --file=FILE --collection=COLLECTION --mongo-db=DB
```
 -a,--action <arg>       What to do if the collection is not empty (append or drop)
 -d,--mongo-db <arg>     Mongo db
 -f,--fields <arg>       Comma separated list of fields (all fields by default)
 -h,--mongo-host <arg>   Mongo host (localhost by default)
 -i,--file <arg>         Input sas file
 -o,--collection <arg>   Output mongo collection
 -p,--mongo-port <arg>   Mongo port (27017 by default)
```
