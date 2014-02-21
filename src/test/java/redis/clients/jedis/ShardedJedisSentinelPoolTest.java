package redis.clients.jedis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.exceptions.JedisConnectionException;

public class ShardedJedisSentinelPoolTest extends TestCase {

	public void testX() throws Exception {
		
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		
		List<String> masters = new ArrayList<String>();
		masters.add("shard1");
		masters.add("shard2");
		
		Set<String> sentinels = new HashSet<String>();
		sentinels.add("192.168.109.212:26379");
		sentinels.add("192.168.109.215:26379");
    
		ShardedJedisSentinelPool pool = new ShardedJedisSentinelPool(masters, sentinels, config, 60000);
		
		ShardedJedis jedis = null;
		try {
			jedis = pool.getResource();
			// do somethind...
			// ...
		} finally {
			if (jedis != null) pool.returnResource(jedis);
			pool.destroy();
		}
		
		ShardedJedis j = null;
		for (int i = 0; i < 100; i++) {
			try {
				j = pool.getResource();
				j.set("KEY: " + i, "" + i);
				System.out.print(i);
				System.out.print(" ");
				Thread.sleep(500);
				pool.returnResource(j);
			} catch (JedisConnectionException e) {
				System.out.print("x");
				i--;
				Thread.sleep(1000);
			}
		}
    
		System.out.println("");
    
		for (int i = 0; i < 100; i++) {
			try {
				j = pool.getResource();
				assertEquals(j.get("KEY: " + i), "" + i);
				System.out.print(".");
				Thread.sleep(500);
				pool.returnResource(j);
			} catch (JedisConnectionException e) {
				System.out.print("x");
				i--;
				Thread.sleep(1000);
			}
		}

		pool.destroy();
	}
}
