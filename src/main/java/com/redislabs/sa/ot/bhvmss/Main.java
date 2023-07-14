package com.redislabs.sa.ot.bhvmss;

import com.redislabs.sa.ot.util.JedisConnectionHelperSettings;
import redis.clients.jedis.JedisPooled;
import com.github.javafaker.Faker;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.params.SetParams;

import java.util.*;

/**
 * * To run the program with the default settings (supplying the host and port for Redis) do:
 *  mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redisDB.centralus.redisenterprise.cache.azure.net --port 10000 --username default --password "
 *
 * Default settings can be found in the code below:
*/
public class Main {

    public static void main(String[] args) {
        List<String> argList = null;
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
        int ttlseconds = 604800; // one week default time before each key expires
        boolean useSSL = false;
        String caCertPath = "";
        String caCertPassword = "";
        String userCertPath = "";
        String userCertPassword = "";

        if (args.length > 0) {
            argList = Arrays.asList(args);
            Iterator<String> argIterator = argList.iterator();
            for (Iterator<String> it = argIterator; it.hasNext(); ) {
                String arg = it.next();
                if (arg.equalsIgnoreCase("--verbose")) {
                    int argIndex = argList.indexOf("--verbose");
                    verbose = Boolean.parseBoolean(argList.get(argIndex + 1));
                }
                if (arg.equalsIgnoreCase("--usehash")) {
                    int argIndex = argList.indexOf("--usehash");
                    useHash = Boolean.parseBoolean(argList.get(argIndex + 1));
                }
                if (arg.equalsIgnoreCase("--host")) {
                    int argIndex = argList.indexOf("--host");
                    host = argList.get(argIndex + 1);
                }
                if (arg.equalsIgnoreCase("--port")) {
                    int argIndex = argList.indexOf("--port");
                    port = Integer.parseInt(argList.get(argIndex + 1));
                }
                if (arg.equalsIgnoreCase("--username")) {
                    int argIndex = argList.indexOf("--username");
                    userName = argList.get(argIndex + 1);
                }
                if (arg.equalsIgnoreCase("--password")) {
                    int argIndex = argList.indexOf("--password");
                    password = argList.get(argIndex + 1);
                }
                if (arg.equalsIgnoreCase("--maxconnections")) {
                    int argIndex = argList.indexOf("--maxconnections");
                    maxConnections = Integer.parseInt(argList.get(argIndex + 1));
                }
                if (arg.equalsIgnoreCase("--howmanyworkers")) {
                    int argIndex = argList.indexOf("--howmanyworkers");
                    numberOfWorkerThreads = Integer.parseInt(argList.get(argIndex + 1));
                }
                if (arg.equalsIgnoreCase("--howmanyparentkeys")) {
                    int argIndex = argList.indexOf("--howmanyparentkeys");
                    howManyParentKeys = Integer.parseInt(argList.get(argIndex + 1));
                }
                if (arg.equalsIgnoreCase("--howmanychildkeys")) {
                    int argIndex = argList.indexOf("--howmanychildkeys");
                    howManyChildKeys = Integer.parseInt(argList.get(argIndex + 1));
                }
                if (arg.equalsIgnoreCase("--workersleeptime")) {
                    int argIndex = argList.indexOf("--workersleeptime");
                    workerSleepTime = Integer.parseInt(argList.get(argIndex + 1));
                }
                if (arg.equalsIgnoreCase("--ttlseconds")) {
                    int argIndex = argList.indexOf("--ttlseconds");
                    ttlseconds = Integer.parseInt(argList.get(argIndex + 1));
                }
                if (arg.equalsIgnoreCase("--usessl")) {
                    useSSL = Boolean.parseBoolean(it.next());
                    System.out.println("loading custom --usessl == " + useSSL);
                }
                if (arg.equalsIgnoreCase("--cacertpath")) {
                    caCertPath = it.next();
                    System.out.println("loading custom --cacertpath == " + caCertPath);
                }
                if (arg.equalsIgnoreCase("--cacertpassword")) {
                    caCertPassword = it.next();
                    System.out.println("loading custom --cacertpassword == " + caCertPassword);
                }
                if (arg.equalsIgnoreCase("--usercertpath")) {
                    userCertPath = it.next();
                    System.out.println("loading custom --usercertpath == " + userCertPath);
                }
                if (arg.equalsIgnoreCase("--usercertpass")) {
                    userCertPassword = it.next();
                    System.out.println("loading custom --usercertpass == " + userCertPassword);
                }
            }
        }
        JedisConnectionHelperSettings settings = new JedisConnectionHelperSettings();
        settings.setRedisHost(host);
        settings.setRedisPort(port);
        settings.setUserName(userName);
        if(password!="") {
            settings.setPassword(password);
            settings.setUsePassword(true);
        }
        settings.setMaxConnections(maxConnections); // these will be healthy, tested connections or idle and removed
        settings.setTestOnBorrow(true);
        settings.setConnectionTimeoutMillis(120000);
        settings.setNumberOfMinutesForWaitDuration(1);
        settings.setNumTestsPerEvictionRun(10);
        settings.setPoolMaxIdle(1); //this means less stale connections
        settings.setPoolMinIdle(0);
        settings.setRequestTimeoutMillis(12000);
        settings.setTestOnReturn(false); // if idle, they will be mostly removed anyway
        settings.setTestOnCreate(true);
        if(useSSL){
            settings.setUseSSL(true);
            settings.setCaCertPath(caCertPath);
            settings.setCaCertPassword(caCertPassword);
            settings.setUserCertPath(userCertPath);
            settings.setUserCertPassword(userCertPassword);
        }
        com.redislabs.sa.ot.util.JedisConnectionHelper connectionHelper = null;
        try{
            connectionHelper = new com.redislabs.sa.ot.util.JedisConnectionHelper(settings); // only use a single connection based on the hostname (not ipaddress) if possible
        }catch(Throwable t){
            t.printStackTrace();
            try{
                Thread.sleep(4000);
            }catch(InterruptedException ie){}
            // give it another go - in case the first attempt was just unlucky:
            connectionHelper = new com.redislabs.sa.ot.util.JedisConnectionHelper(settings); // only use a single connection based on the hostname (not ipaddress) if possible
        }

        long mainThreadExecutionStartTime = System.currentTimeMillis();
        System.out.println("Program total execution time is");
        System.out.println("\nBEFORE WE BEGIN DATABASE HAS THIS MANY KEYS IN IT: "+connectionHelper.getPooledJedis().dbSize());
        if (useHash) {
            for (int x = 0; x < numberOfWorkerThreads; x++) {
                try { Thread.sleep(500);}catch(Throwable t){}
                HashWriter hashWriter = new HashWriter()
                        .setTotalNumberToWrite(howManyParentKeys)
                        .setHowManyNestedKeys(howManyChildKeys)
                        .setJedisPooled(connectionHelper.getPooledJedis())
                        .setSleepTime(workerSleepTime)
                        .setVerbose(verbose);
                new Thread(hashWriter).start();
            }
        }else {
            for (int x = 0; x < numberOfWorkerThreads; x++) {
                try { Thread.sleep(500);}catch(Throwable t){}
                StringWriter stringWriter = new StringWriter()
                        .setTotalNumberToWrite(howManyParentKeys)
                        .setHowManyNestedKeys(howManyChildKeys)
                        .setJedisPooled(connectionHelper.getPooledJedis())
                        .setSleepTime(workerSleepTime)
                        .setTTLSeconds(ttlseconds)
                        .setVerbose(verbose);
                new Thread(stringWriter).start();
            }
        }
        System.out.println("Main Program method exiting - wall clock start time [measured in SECONDS] was: "+mainThreadExecutionStartTime/1000);
        System.out.println("Subtract the Main start wall time from the last reported thread wall time to see how many SECONDS the program took to execute");
    }
}

class StringWriter implements Runnable {
    private JedisPooled jedisPooled = null;
    private long totalNumberToWrite = 0l;
    private long sleepTime = 0l;
    private long howManyNestedKeys = 10;
    private boolean verbose = false;
    private int ttlSeconds = 0; // time before each key expires
    Faker faker = new Faker();

    public StringWriter setJedisPooled(JedisPooled jedis){
        this.jedisPooled=jedis;
        return this;
    }

    public StringWriter setTTLSeconds(int ttlSeconds){
        this.ttlSeconds=ttlSeconds;
        return this;
    }

    public StringWriter setVerbose(boolean verbose){
        this.verbose=verbose;
        return this;
    }


    public StringWriter setHowManyNestedKeys(long howManyNestedKeys){
        this.howManyNestedKeys=howManyNestedKeys;
        return this;
    }

    public StringWriter setTotalNumberToWrite(long totalNumberToWrite){
        this.totalNumberToWrite=totalNumberToWrite;
        return this;
    }

    public StringWriter setSleepTime(long sleepTime){
        this.sleepTime=sleepTime;
        return this;
    }

    @Override
    public void run() {
        if(verbose){
            System.out.println("About to begin another worker Thread.  Every '.' printed represents "+howManyNestedKeys+" String objects written to Redis\n");
        }
        long startTime = System.currentTimeMillis();
        for(int x=0;x<totalNumberToWrite;x++){
            try(Pipeline pipeline = new Pipeline(jedisPooled.getPool().getResource());) {
                long pipelineBuildStartTime = System.currentTimeMillis();
                SetParams setParams = new SetParams().ex(ttlSeconds); // time before each key expires
                for (int m = 0; m < howManyNestedKeys; m++) {
                    String compNameID = faker.company().industry() + ":" + faker.company().name() + ":" + faker.idNumber()+System.currentTimeMillis();
                    if(m%5==0){
                        compNameID = "owen"+faker.company().industry() + ":" + faker.company().name() + ":" + faker.idNumber()+System.currentTimeMillis();
                    }
                    pipeline.set(compNameID + ":" + x, faker.pokemon().name() + ":" + compNameID + ":" + x,setParams);
                }
                if (verbose && (x == 0)) {
                    System.out.println("building a pipeline object with " + howManyNestedKeys + " took " + (System.currentTimeMillis() - pipelineBuildStartTime) + " milliseconds");
                }
                pipeline.sync();
            }
            try{
                Thread.sleep(sleepTime);
                if((x%10==0)&&verbose){
                    System.out.print(".");
                }
            }catch(InterruptedException ie){ie.getMessage();}
        }
        System.out.println("\nMaking "+totalNumberToWrite+" Pipeline calls each containing "+howManyNestedKeys+" Strings into Redis took "+(System.currentTimeMillis()-startTime));
        System.out.println("\nWall clock stop time for this thread [measured in SECONDS] is "+System.currentTimeMillis()/1000);
    }
}

class HashWriter implements Runnable{
    private JedisPooled jedisPooled = null;
    private long totalNumberToWrite = 0l;
    private long sleepTime = 0l;
    private long howManyNestedKeys=10;
    private boolean verbose=false;
    Faker faker = new Faker();

    public HashWriter setJedisPooled(JedisPooled jedis){
        this.jedisPooled=jedis;
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
            System.out.println("About to begin another worker Thread.  Every '.' printed represents 10 Hash objects written to Redis\n");
        }
        long startTime = System.currentTimeMillis();
        for(int x=0;x<totalNumberToWrite;x++){
            String keyName = faker.company().name()+":"+faker.company().industry()+":"+faker.idNumber()+":"+faker.pokemon().name()+":"+x;
            HashMap<String,String> valuesMap = new HashMap<>();
            long mapBuildStartTime = System.currentTimeMillis();
            for(int m=0;m<howManyNestedKeys;m++) {
                valuesMap.put( faker.company().name()+":"+faker.company().industry()+":"+faker.idNumber()+":"+faker.pokemon().name()+":"+x, faker.company().name()+":"+faker.company().industry()+":"+faker.idNumber()+":"+faker.pokemon().name()+":"+x);
            }
            if(verbose&&(x==0)){
                System.out.println("building a map object with "+howManyNestedKeys+" took "+(System.currentTimeMillis()-mapBuildStartTime)+" milliseconds");
            }
            jedisPooled.hset(keyName,valuesMap);
            try{
                Thread.sleep(sleepTime);
                if((x%10==0)&&verbose){
                    System.out.print(".");
                }
            }catch(InterruptedException ie){ie.getMessage();}
        }
        System.out.println("\nWriting "+totalNumberToWrite+" Hash objects each containing "+howManyNestedKeys+" into Redis took "+(System.currentTimeMillis()-startTime));
        System.out.println("\nWall clock stop time for this thread measured in seconds is "+System.currentTimeMillis()/1000);
    }
}
