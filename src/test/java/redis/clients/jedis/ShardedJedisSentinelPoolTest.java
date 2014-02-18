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
		final Set<String> sentinels = new HashSet<String>();
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();

		sentinels.add("192.168.108.145:26379");
//    sentinels.add("127.0.0.1:26379");
//    sentinels.add("127.0.0.1:26380");
//    sentinels.add("127.0.0.1:26381");
    
		List<String> masters = new ArrayList<String>();
		masters.add("mymaster");
		masters.add("mymaster2");

		ShardedJedisSentinelPool pool = new ShardedJedisSentinelPool(masters, sentinels, config, 90000, null, 0);

		ShardedJedis j = pool.getResource();
		pool.returnResource(j);

		for (int i = 0; i < 100; i++) {
			try {
				j = pool.getResource();
				j.set("KEY: " + i, "" + i);
				System.out.print(i);
				System.out.print(" ");
//	    System.out.print(".");
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
//    	    System.out.println("KEY[" + i + "]: " + j.get("KEY: " + i));
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
