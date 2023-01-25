package com.redislabs.sa.ot.bhvmss;

import redis.clients.jedis.JedisPooled;
import com.github.javafaker.Faker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * * To run the program with the default settings (supplying the host and port for Redis) do:
 *  mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redisDB.centralus.redisenterprise.cache.azure.net --port 10000 --username default --password "
 *
 * Default settings can be found in the code below:
*/
public class Main {

    public static void main(String[] args) {
        ArrayList<String> argList = null;
        String host = "localhost";
        int port = 6379;
        String userName = "default";
        String password = "";
        int maxConnections = 20;
        boolean verbose = false; // in case we want to add debugging statements
        long numberOfWorkerThreads = 20;
        long howManyParentKeys = 10000;
        long howManyChildKeys = 100;
        long workerSleepTime = 10000;//milliseconds
        boolean useHash = false;

        if (args.length > 0) {
            argList = new ArrayList<>(Arrays.asList(args));
            if (argList.contains("--verbose")) {
                int argIndex = argList.indexOf("--verbose");
                verbose = Boolean.parseBoolean(argList.get(argIndex + 1));
            }
            if (argList.contains("--usehash")) {
                int argIndex = argList.indexOf("--usehash");
                useHash = Boolean.parseBoolean(argList.get(argIndex + 1));
            }
            if (argList.contains("--host")) {
                int argIndex = argList.indexOf("--host");
                host = argList.get(argIndex + 1);
            }
            if (argList.contains("--port")) {
                int argIndex = argList.indexOf("--port");
                port = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--username")) {
                int argIndex = argList.indexOf("--username");
                userName = argList.get(argIndex + 1);
            }
            if (argList.contains("--password")) {
                int argIndex = argList.indexOf("--password");
                password = argList.get(argIndex + 1);
            }
            if (argList.contains("--maxconnections")) {
                int argIndex = argList.indexOf("--maxconnections");
                maxConnections = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--howmanyworkers")) {
                int argIndex = argList.indexOf("--howmanyworkers");
                numberOfWorkerThreads = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--howmanyparentkeys")) {
                int argIndex = argList.indexOf("--howmanyparentkeys");
                howManyParentKeys = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--howmanychildkeys")) {
                int argIndex = argList.indexOf("--howmanychildkeys");
                howManyChildKeys = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--workersleeptime")) {
                int argIndex = argList.indexOf("--workersleeptime");
                workerSleepTime = Integer.parseInt(argList.get(argIndex + 1));
            }

        }

        JedisConnectionHelper connectionHelper = new JedisConnectionHelper(host, port, userName, password, maxConnections);
        if (useHash) {
            for (int x = 0; x < numberOfWorkerThreads; x++) {
                HashWriter hashWriter = new HashWriter()
                        .setTotalNumberToWrite(howManyParentKeys)
                        .setHowManyNestedKeys(howManyChildKeys)
                        .setJedisPooled(connectionHelper.getPooledJedis())
                        .setSleepTime(workerSleepTime)
                        .setVerbose(verbose);
                new Thread(hashWriter).start();
            }
        }
    }
}

class HashWriter implements Runnable{
    private JedisPooled jedis = null;
    private long totalNumberToWrite = 0l;
    private long sleepTime = 0l;
    private long howManyNestedKeys=10;
    private boolean verbose=false;
    Faker faker = new Faker();

    public HashWriter setJedisPooled(JedisPooled jedis){
        this.jedis=jedis;
        return this;
    }

    public HashWriter setVerbose(boolean verbose){
        this.verbose=verbose;
        return this;
    }


    public HashWriter setHowManyNestedKeys(long howManyNestedKeys){
        this.howManyNestedKeys=howManyNestedKeys;
        return this;
    }

    public HashWriter setTotalNumberToWrite(long totalNumberToWrite){
        this.totalNumberToWrite=totalNumberToWrite;
        return this;
    }

    public HashWriter setSleepTime(long sleepTime){
        this.sleepTime=sleepTime;
        return this;
    }

    @Override
    public void run() {
        if(verbose){
            System.out.println("About to begin another worker Thread.  Every '.' printed represents 100 Hash objects written to Redis\n");
        }
        long startTime = System.currentTimeMillis();
        for(int x=0;x<totalNumberToWrite;x++){
            String keyName = faker.company().name()+":"+faker.company().industry()+":"+faker.idNumber()+":"+faker.pokemon().name()+":"+x;
            HashMap<String,String> valuesMap = new HashMap<>();
            for(int m=0;m<howManyNestedKeys;m++) {
                valuesMap.put( faker.company().name()+":"+faker.company().industry()+":"+faker.idNumber()+":"+faker.pokemon().name()+":"+x, faker.company().name()+":"+faker.company().industry()+":"+faker.idNumber()+":"+faker.pokemon().name()+":"+x);
            }
            jedis.hset(keyName,valuesMap);
            try{
                Thread.sleep(sleepTime);
                if((x%100==0)&&verbose){
                    System.out.print(".");
                }
            }catch(InterruptedException ie){ie.getMessage();}
        }
        System.out.println("\nWriting "+totalNumberToWrite+" Hash objects each containing "+howManyNestedKeys+" into Redis took "+(System.currentTimeMillis()-startTime));
    }
}
