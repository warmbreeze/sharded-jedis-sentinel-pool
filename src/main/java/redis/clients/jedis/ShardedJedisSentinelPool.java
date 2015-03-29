package redis.clients.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.util.Hashing;
import redis.clients.util.Pool;

import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ShardedJedisSentinelPool extends Pool<ShardedJedis> {

	public static final int MAX_RETRY_SENTINEL = 10;
	
	protected final Logger log = Logger.getLogger(getClass().getName());
	
	protected GenericObjectPoolConfig poolConfig;

    protected int timeout = Protocol.DEFAULT_TIMEOUT;
    
    private int sentinelRetry = 0;

    protected String password;

    protected int database = Protocol.DEFAULT_DATABASE;

    protected Set<MasterListener> masterListeners = new HashSet<MasterListener>();
    
    volatile List<HostMaster> currentHostMasters;
    
    public ShardedJedisSentinelPool(List<String> masters, Set<String> sentinels) {
		this(masters, sentinels, new GenericObjectPoolConfig(),
			Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
    }
    
    public ShardedJedisSentinelPool(List<String> masters, Set<String> sentinels, String password) {
		this(masters, sentinels, new GenericObjectPoolConfig(),
			Protocol.DEFAULT_TIMEOUT, password);
    }
    
    public ShardedJedisSentinelPool(final GenericObjectPoolConfig poolConfig, List<String> masters, Set<String> sentinels) {
		this(masters, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null,
			Protocol.DEFAULT_DATABASE);
    }

    public ShardedJedisSentinelPool(List<String> masters, Set<String> sentinels,
	    final GenericObjectPoolConfig poolConfig, int timeout,
	    final String password) {
		this(masters, sentinels, poolConfig, timeout, password,
			Protocol.DEFAULT_DATABASE);
    }

    public ShardedJedisSentinelPool(List<String> masters, Set<String> sentinels,
	    final GenericObjectPoolConfig poolConfig, final int timeout) {
		this(masters, sentinels, poolConfig, timeout, null,
			Protocol.DEFAULT_DATABASE);
    }

    public ShardedJedisSentinelPool(List<String> masters, Set<String> sentinels,
	    final GenericObjectPoolConfig poolConfig, final String password) {
		this(masters, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT,
			password);
    }

    public ShardedJedisSentinelPool(List<String> masters, Set<String> sentinels,
	    final GenericObjectPoolConfig poolConfig, int timeout,
	    final String password, final int database) {
		this.poolConfig = poolConfig;
		this.timeout = timeout;
		this.password = password;
		this.database = database;

		List<HostMaster> masterList = initSentinels(sentinels, masters);
		initPool(masterList);
    }

    public void destroy() {
		for (MasterListener m : masterListeners) {
		    m.shutdown();
		}
		
		super.destroy();
    }

    public List<HostAndPort> getCurrentHostMaster() {
    	return currentHostMasters.stream().map(HostMaster::getHostAndPort).collect(Collectors.toList());
    }

    void initPool(List<HostMaster> masters) {
    	if (!isPoolInitialized(currentHostMasters, masters)) {
    		StringBuffer sb = new StringBuffer();
    		for (HostMaster master : masters) {
    			sb.append(master.getHostAndPort().toString());
				sb.append("->");
				sb.append(master.getName());
    			sb.append(" ");
    		}
    		log.info("Created ShardedJedisPool to master at [" + sb.toString() + "]");
    		List<JedisShardInfo> shardMasters = makeShardInfoList(masters);
    		initPool(poolConfig, new ShardedJedisFactory(shardMasters, Hashing.MURMUR_HASH, null));
    		currentHostMasters = masters;
    	}
    }

    private boolean isPoolInitialized(List<HostMaster> currentShardMasters, List<HostMaster> shardMasters) {
    	if (currentShardMasters != null && shardMasters != null) {
    		if (currentShardMasters.size() == shardMasters.size()) {
    			for (int i = 0; i < currentShardMasters.size(); i++) {
    				if (!currentShardMasters.get(i).equals(shardMasters.get(i))) return false;
    			}
    			return true;
    		}
    	}
		return false;
	}

	private List<JedisShardInfo> makeShardInfoList(List<HostMaster> masters) {
		List<JedisShardInfo> shardMasters = new ArrayList<JedisShardInfo>();
		for (HostMaster master : masters) {
			JedisShardInfo jedisShardInfo = new JedisShardInfo(master.getHostAndPort().getHost(),
					master.getHostAndPort().getPort(), timeout, master.getName());
			jedisShardInfo.setPassword(password);
			
			shardMasters.add(jedisShardInfo);
		}
		return shardMasters;
	}

	private List<HostMaster> initSentinels(Set<String> sentinels, final List<String> masters) {

    	Map<String, HostAndPort> masterMap = new HashMap<>();
    	List<HostMaster> shardMasters = new ArrayList<>();
    	
	    log.info("Trying to find all master from available Sentinels...");
	    
	    for (String masterName : masters) {
	    	HostAndPort master = null;
	    	boolean fetched = false;
	    	
	    	while (!fetched && sentinelRetry < MAX_RETRY_SENTINEL) {
	    		for (String sentinel : sentinels) {
					final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));

					log.fine("Connecting to Sentinel " + hap);
			
					try {
					    Jedis jedis = new Jedis(hap.getHost(), hap.getPort());
					    master = masterMap.get(masterName);
					    if (master == null) {
					    	List<String> hostAndPort = jedis.sentinelGetMasterAddrByName(masterName);
					    	if (hostAndPort != null && hostAndPort.size() > 0) {
					    		master = toHostAndPort(hostAndPort);
								log.fine("Found Redis master at " + master);
								shardMasters.add(new HostMaster(master, masterName));
								masterMap.put(masterName, master);
								fetched = true;
								jedis.disconnect();
								break;
					    	}
					    }
					} catch (JedisConnectionException e) {
					    log.warning("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
					}
		    	}
		    	
		    	if (null == master) {
		    		try {
						log.severe("All sentinels down, cannot determine where is "
							+ masterName + " master is running... sleeping 1000ms, Will try again.");
						Thread.sleep(1000);
				    } catch (InterruptedException e) {
				    	e.printStackTrace();
				    }
		    		fetched = false;
		    		sentinelRetry++;
		    	}
	    	}
	    	
	    	// Try MAX_RETRY_SENTINEL times.
	    	if (!fetched && sentinelRetry >= MAX_RETRY_SENTINEL) {
	    		log.severe("All sentinels down and try " + MAX_RETRY_SENTINEL + " times, Abort.");
	    		throw new JedisConnectionException("Cannot connect all sentinels, Abort.");
	    	}
	    }
	    
	    // All shards master must been accessed.
	    if (masters.size() != 0 && masters.size() == shardMasters.size()) {
	    	
	    	log.info("Starting Sentinel listeners...");
			for (String sentinel : sentinels) {
			    final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
			    MasterListener masterListener = new MasterListener(this, masters, hap.getHost(), hap.getPort());
			    masterListeners.add(masterListener);
			    masterListener.start();
			}
	    }
	    
		return shardMasters;
    }

    HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
    	String host = getMasterAddrByNameResult.get(0);
    	int port = Integer.parseInt(getMasterAddrByNameResult.get(1));
    	
    	return new HostAndPort(host, port);
    }

}
