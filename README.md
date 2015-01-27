Sharded Jedis Sentinel Pool
===========================

Sharded Jedis Sentinel Pool used to obtain ShardedJedis resources from HA Redis cluster

### Usage
Functionally this resource pool is a merge of Sentinel Pool and Sharded Pool.

You can create it f.e. like that:
```java
//you need a list of master names as registered at Sentinels
final List<String> masters = Arrays.asList("shard1", "shard2");

//and a list of Sentinels themselves
final Set<String> sentinels = Sets.newHashSet("localhost:26379", "localhost:26380", "localhost:26381");

//the masters host:ports will be automatically resolved against Sentinels and shards initialized
ShardedJedisSentinelPool pool = new ShardedJedisSentinelPool(masters, sentinels, "password");
```

Once you create a ```ShardedJedisSentinelPool``` you can use it the same way as you would before:
```java
try {
    jedis = pool.getResource();
    //(...)
    jedis.set("jestem", "sliczny");
    jedis.get("jestem");
    //(...)
} finally {
    if (jedis != null) pool.returnResource(jedis);
}

//... eventually ...
pool.destroy();
```