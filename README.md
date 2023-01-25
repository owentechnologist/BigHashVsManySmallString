## To run the program with the default settings (supplying the host and port for Redis) do:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redisDB.centralus.redisenterprise.cache.azure.net --port 10000 --username default --password "
```

### Optional Settings with sample values are:
--usehash true
--maxconnections 50
--howmanyworkers 50
--howmanyparentkeys 10000
--howmanychildkeys 100
--workersleeptime 20000
--verbose true

```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--workersleeptime 2000 --howmanychildkeys 1000 --howmanyparentkeys 1000 --howmanyworkers 100 --maxconnections 110 --usehash true --port 10000 --username default --host redisDB.centralus.redisenterprise.cache.azure.net --password iU3cfKympxOScuHjODiBxBW4w35iNdFrNXO7xKDDHqU="
```

