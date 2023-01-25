## Writing data to Redis involves network and other resources
### this program will allow playing with different configurations of Hash objects
### it deliberately does not use Pipelining
### making a really big hash is similar to pipelining but has the downside of 
### blocking on the redis side for as long as it takes to make sure redis gets all the data in the one big key

#### In this example, if you set the following: 
``` 
--workersleeptime 50 
--howmanychildkeys 10000 
--howmanyparentkeys 10 
--howmanyworkers 10 
--maxconnections 10 
--usehash true 
```
### You will write 2MB Hash objects 10 * 10 times (with 10 threads competing for your network at the same time)
### 100 * 2MB == 200MB and it takes roughly a minute on my home wifi using my laptop.
### I also notice a /second outbound in my laptop nic card of roughly 6 MB
### Note that it also takes about just under 2 seconds to create each of the Hash objects (as they each contain 10K sub-keys).
### Azure Enterprise Redis Server-side maxes out at roughly 60MB/second inbound
## server-side measured latency per write is roughly 30 milliseconds

### You can also try writing the same 200MB of data this way:
* place 1000 child keys in each object instead of 10000
* write with 40 worker threads (instead of 10)
* write 25 of the objects per thread instead of 10  
### Results:
* Building each map takes < 75 milliseconds
* writing the total 200 MB data set takes < 45 seconds
### I notice a /second outbound in my laptop nic card of roughly 4MB
## server-side measured latency per write is roughly 1 millisecond

## If you needed to interpolate other operations like reads etc... 
## it would be beneficial to use the smaller object sizes (roughly 200KB/object) 
## this likely has something to do with default packet sizes on the network and other such things

## You can also write 5X the data by building 200 workers that utilize 200 connections...
## it appears that maximizing the parallel # of packets you can write on the wire is the key
## 5X the data took 223 seconds or pretty much exactly 5X the time


## To run the program with the default settings (supplying the host and port for Redis) do:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redisDB.centralus.redisenterprise.cache.azure.net --port 10000 --username default --password "
```

### Again: Optional Settings with sample values are:
--usehash true
--maxconnections 50
--howmanyworkers 50
--howmanyparentkeys 1000
--howmanychildkeys 1000
--workersleeptime 50
--verbose true
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--workersleeptime 50 --howmanychildkeys 1000 --howmanyparentkeys 1000 --howmanyworkers 50 --maxconnections 50 --usehash true --port 10000 --username default --host redisDB.centralus.redisenterprise.cache.azure.net --password iU3cfKympxOScuHjODiBxBW4w35iNdFrNXO7xKDDHqU="
```

