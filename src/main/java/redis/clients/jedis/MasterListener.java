package redis.clients.jedis;

import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
* Created by piotrturek on 29/03/15.
*/
class MasterListener extends Thread {

    private ShardedJedisSentinelPool shardedJedisSentinelPool;
    protected List<String> masters;
    protected String host;
    protected int port;
    protected long subscribeRetryWaitTimeMillis = 5000;
    protected Jedis jedis;
    protected AtomicBoolean running = new AtomicBoolean(false);

    protected MasterListener(ShardedJedisSentinelPool shardedJedisSentinelPool) {
        this.shardedJedisSentinelPool = shardedJedisSentinelPool;
    }

    public MasterListener(ShardedJedisSentinelPool shardedJedisSentinelPool, List<String> masters, String host, int port) {
        this.shardedJedisSentinelPool = shardedJedisSentinelPool;
        this.masters = masters;
        this.host = host;
        this.port = port;
    }

    public MasterListener(ShardedJedisSentinelPool shardedJedisSentinelPool, List<String> masters, String host, int port,
						  long subscribeRetryWaitTimeMillis) {
        this(shardedJedisSentinelPool, masters, host, port);
        this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
    }

    public void run() {

        running.set(true);

        while (running.get()) {

        jedis = new Jedis(host, port);
        try {
            jedis.subscribe(new JedisPubSubAdapter() {
                @Override
                public void onMessage(String channel, String message) {
                    shardedJedisSentinelPool.log.info("Sentinel " + host + ":" + port + " published: " + message + ".");

                    String[] switchMasterMsg = message.split(" ");

                    if (switchMasterMsg.length > 3) {

                        int index = masters.indexOf(switchMasterMsg[0]);
                        if (index >= 0) {
                            HostAndPort newHostAndPort = shardedJedisSentinelPool.toHostAndPort(Arrays.asList(switchMasterMsg[3], switchMasterMsg[4]));
                            HostMaster newHostMaster = new HostMaster(newHostAndPort, masters.get(index));
                            List<HostMaster> newHostMasters = new ArrayList<>();
                            for (int i = 0; i < masters.size(); i++) {
                                newHostMasters.add(null);
                            }
                            Collections.copy(newHostMasters, shardedJedisSentinelPool.currentHostMasters);
                            newHostMasters.set(index, newHostMaster);

                            shardedJedisSentinelPool.initPool(newHostMasters);
                        } else {
                            StringBuffer sb = new StringBuffer();
                            for (String masterName : masters) {
                                sb.append(masterName);
                                sb.append(",");
                            }
                            shardedJedisSentinelPool.log.info("Ignoring message on +switch-master for master name "
                                    + switchMasterMsg[0]
                                    + ", our monitor master name are ["
                                    + sb + "]");
                        }

                    } else {
                        shardedJedisSentinelPool.log.severe("Invalid message received on Sentinel "
                            + host
                            + ":"
                            + port
                            + " on channel +switch-master: "
                            + message);
                    }
                }
            }, "+switch-master");

        } catch (JedisConnectionException e) {

            if (running.get()) {
                shardedJedisSentinelPool.log.severe("Lost connection to Sentinel at " + host
                    + ":" + port
                    + ". Sleeping 5000ms and retrying.");
                try {
                    Thread.sleep(subscribeRetryWaitTimeMillis);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } else {
                shardedJedisSentinelPool.log.info("Unsubscribing from Sentinel at " + host + ":"
                        + port);
            }
        }
        }
    }

    public void shutdown() {
        try {
            shardedJedisSentinelPool.log.info("Shutting down listener on " + host + ":" + port);
            running.set(false);
            // This isn't good, the Jedis object is not thread safe
            jedis.disconnect();
        } catch (Exception e) {
            shardedJedisSentinelPool.log.severe("Caught exception while shutting down: " + e.getMessage());
        }
    }
}
