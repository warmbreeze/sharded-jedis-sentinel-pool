package redis.clients.jedis;

import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Created by piotrturek on 30/03/15.
 */
public interface MasterListenerErrorHandler {
    void handleError(JedisConnectionException e, boolean running);
}
