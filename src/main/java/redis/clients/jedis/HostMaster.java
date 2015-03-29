package redis.clients.jedis;

/**
* Created by piotrturek on 29/03/15.
*/
class HostMaster {
    private final HostAndPort hostAndPort;
    private final String name;

    public HostAndPort getHostAndPort() {
        return hostAndPort;
    }

    public String getName() {
        return name;
    }

    HostMaster(HostAndPort hostAndPort, String name) {
        this.hostAndPort = hostAndPort;
        this.name = name;
    }
}
