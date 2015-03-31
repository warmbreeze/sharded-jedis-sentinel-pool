package redis.clients.jedis;

import com.google.common.collect.Sets;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.embedded.RedisCluster;
import redis.embedded.RedisSentinel;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.fest.assertions.Assertions.assertThat;

public class ShardedJedisSentinelPoolTest {
	private static final int PORT_OF_SECOND_SHARD = 6383;
	private RedisCluster cluster;
	private ShardedJedisSentinelPool pool;

	@Before
	public void setUp() throws Exception {
		cluster = RedisCluster.builder().withSentinelBuilder(RedisSentinel.builder().downAfterMilliseconds(1000L).failoverTimeout(1000L))
				.sentinelCount(3).quorumSize(1)
				.replicationGroup("shard1", 3)
				.replicationGroup("shard2", 3)
				.build();
		cluster.start();
		final GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		final List<String> masters = Arrays.asList("shard1", "shard2");
		final Set<String> sentinels = Sets.newHashSet("localhost:26379", "localhost:26380", "localhost:26381");

		pool = new ShardedJedisSentinelPool(masters, sentinels, config, 60000);
	}

	@Test
	public void shouldInitPoolProperly() throws Exception {
		//given

		//when
		ShardedJedis jedis = null;
		try {
			jedis = pool.getResource();
		} finally {
			if (jedis != null) pool.returnResource(jedis);
			pool.destroy();
		}

		//then, nothing should explode
	}

	@Test
	public void shouldAllowToDoASimplePut() throws Exception {
		//given
		final String key = "key";
		final String value = "value";

		ShardedJedis jedis = null;
		try {
			//when
			jedis = pool.getResource();
			jedis.set(key, value);
			final String result = jedis.get(key);

			//then
			assertThat(result).isEqualTo(value);
		} finally {
			if (jedis != null) pool.returnResource(jedis);
			pool.destroy();
		}
	}

	@Test
	public void shouldHaveAllShardsInitializedProperly() throws Exception {
		//given

		ShardedJedis jedis = null;
		try {
			//when
			jedis = pool.getResource();
			final Collection<Jedis> allShards = jedis.getAllShards();

			//then
			assertThat(allShards).hasSize(2);
		} finally {
			if (jedis != null) pool.returnResource(jedis);
			pool.destroy();
		}
	}

	@Test
	public void shardsInfosShouldHaveNamesPropagated() throws Exception {
		//given

		ShardedJedis jedis = null;
		try {
			//when
			jedis = pool.getResource();

			//then
			assertThat(jedis.getAllShardInfo().stream().map(JedisShardInfo::getName).anyMatch(n -> null == n)).isFalse();
			assertThat(jedis.getAllShardInfo().stream().map(JedisShardInfo::getName).allMatch(n -> n.equals("shard1") || n.equals("shard2"))).isTrue();
		} finally {
			if (jedis != null) pool.returnResource(jedis);
			pool.destroy();
		}
	}

	@Test
	public void shouldDistributeEntriesAcrossShards() throws Exception {
		//given
		final List<String> keyValues = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n");

		ShardedJedis jedis = null;
		try {
			//when
			jedis = pool.getResource();
			for (String kv : keyValues) {
				jedis.set(kv, kv);
			}

			//then
			final ShardedJedis finalJedis = jedis;
			final Map<JedisShardInfo, Integer> fillFactors = new HashMap<>();
			keyValues.stream().map(finalJedis::getShardInfo).forEach(i -> {
				final Integer v = fillFactors.getOrDefault(i, 0);
				fillFactors.put(i, v + 1);
			});
			final Integer minCount = Collections.min(fillFactors.values());
			final Integer maxCount = Collections.max(fillFactors.values());

			assertThat(minCount * 2).as("The least populated shard should have at most two times less entries than the most populated one")
					.isGreaterThanOrEqualTo(maxCount);
		} finally {
			if (jedis != null) pool.returnResource(jedis);
			pool.destroy();
		}
	}

	@Test
	public void shouldRecoverFromMasterFailover() throws Exception {
		//given
		final ShardedJedis preFailover = pool.getResource();
		final int preFailoverPort = preFailover.getAllShardInfo().stream().map(JedisShardInfo::getPort).filter(p -> p != PORT_OF_SECOND_SHARD).findFirst().get();

		//when
		try {
			//force manual failover by stopping master of first shard (has to be done this way cause of: https://github.com/antirez/redis/issues/1651)
			cluster.servers().get(0).stop();
			TimeUnit.SECONDS.sleep(5);

			//then
			final ShardedJedis afterFailover = pool.getResource();
			final int afterFailoverPort = afterFailover.getAllShardInfo().stream().map(JedisShardInfo::getPort).filter(p -> p != PORT_OF_SECOND_SHARD).findFirst().get();

			//assert that master port of the first shard changed after manually triggering failover
			assertThat(preFailoverPort).isNotEqualTo(afterFailoverPort);
		} finally {
			pool.destroy();
		}
	}

	@After
	public void tearDown() throws Exception {
		cluster.stop();
	}
}
