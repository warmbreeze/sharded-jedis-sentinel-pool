package redis.clients.jedis;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import redis.clients.util.Hashing;

import java.util.List;
import java.util.regex.Pattern;

/**
 * PoolableObjectFactory custom impl.
 */
class ShardedJedisFactory implements PooledObjectFactory<ShardedJedis> {
    private List<JedisShardInfo> shards;
    private Hashing algo;
    private Pattern keyTagPattern;

    public ShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern) {
        this.shards = shards;
        this.algo = algo;
        this.keyTagPattern = keyTagPattern;
    }

    public PooledObject<ShardedJedis> makeObject() throws Exception {
        ShardedJedis jedis = new ShardedJedis(shards, algo, keyTagPattern);
        return new DefaultPooledObject<ShardedJedis>(jedis);
    }

    public void destroyObject(PooledObject<ShardedJedis> pooledShardedJedis) throws Exception {
        final ShardedJedis shardedJedis = pooledShardedJedis.getObject();
        for (Jedis jedis : shardedJedis.getAllShards()) {
        try {
            try {
            jedis.quit();
            } catch (Exception e) {

            }
            jedis.disconnect();
        } catch (Exception e) {

        }
        }
    }

    public boolean validateObject(PooledObject<ShardedJedis> pooledShardedJedis) {
        try {
        ShardedJedis jedis = pooledShardedJedis.getObject();
        for (Jedis shard : jedis.getAllShards()) {
            if (!shard.ping().equals("PONG")) {
            return false;
            }
        }
        return true;
        } catch (Exception ex) {
        return false;
        }
    }

    public void activateObject(PooledObject<ShardedJedis> p) throws Exception {

    }

    public void passivateObject(PooledObject<ShardedJedis> p) throws Exception {

    }
}
