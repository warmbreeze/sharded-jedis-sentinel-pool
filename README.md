Until [#370 @ xetorthio/jedis](/xetorthio/jedis/pull/370) is not merged into jedis master branch you have to use [my fork](/hamsterready/jedis).

# Testing
 1. Follow instructions @[noise/redis-sentinel-tests](/noise/redis-sentinel-tests) to setup redis master/slave and sentinels.
 2. Strat [SentinelBasedJedisPoolWrapperTest](/hamsterready/jedis-sentinel-pool/blob/master/src/test/java/pl/quaternion/SentinelBasedJedisPoolWrapperTest.java).
 3. Stop master.

# License
Apache2.0
