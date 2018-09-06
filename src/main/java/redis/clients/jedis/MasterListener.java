package redis.clients.jedis;

import redis.clients.jedis.errors.SilentErrorHandler;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
* Created by piotrturek on 29/03/15.
*/
class MasterListener extends Thread {
    private ShardedJedisSentinelPool shardedJedisSentinelPool;
    protected List<String> masters;
    protected String host;
    protected int port;
    protected Jedis jedis;
    protected AtomicBoolean running = new AtomicBoolean(false);
    private MasterListenerErrorHandler errorHandler;

    protected MasterListener(ShardedJedisSentinelPool shardedJedisSentinelPool) {
        this.shardedJedisSentinelPool = shardedJedisSentinelPool;
    }

    public MasterListener(ShardedJedisSentinelPool shardedJedisSentinelPool, List<String> masters, String host, int port) {
        this.shardedJedisSentinelPool = shardedJedisSentinelPool;
        this.masters = masters;
        this.host = host;
        this.port = port;
        this.errorHandler = new SilentErrorHandler("Sentinel at " + host + " and port " + port, shardedJedisSentinelPool.log);
        setName("RedisMasterListener-" + host + "-" + port);
    }

    public MasterListener(ShardedJedisSentinelPool shardedJedisSentinelPool, List<String> masters, String host, int port,
						  long subscribeRetryWaitTimeMillis) {
        this(shardedJedisSentinelPool, masters, host, port);
    }

    @Override
    public void run() {
        running.set(true);
        while (running.get()) {
            try {
                trySubscribeToSentinel();
            } catch (Exception e) {
                shardedJedisSentinelPool.log.log(Level.SEVERE, "caught unexpected exception to prevent MasterListener thread from dying", e);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                }
            }
        }
    }

    private void trySubscribeToSentinel() {
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
            errorHandler.handleError(e, running.get());
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
