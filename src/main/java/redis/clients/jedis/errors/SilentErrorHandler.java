package redis.clients.jedis.errors;

import redis.clients.jedis.MasterListenerErrorHandler;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created by piotrturek on 30/03/15.
 */
public class SilentErrorHandler implements MasterListenerErrorHandler {
    private final Logger log;
    private static final long DEFAULT_SUBSCRIBE_RETRY_WAIT_TIME_MILLIS = 5000;
    private static final long DEFAULT_SHOUTING_THRESHOLD_MILLIS = 10000;
    private String sentinelDescription;
    private final long subscribeRetryWaitTimeMillis;
    private long shoutingThresholdMillis;
    private long lastErrorTimeMillis = -1;

    public SilentErrorHandler(String description, Logger log) {
        this(description, log, DEFAULT_SUBSCRIBE_RETRY_WAIT_TIME_MILLIS, DEFAULT_SHOUTING_THRESHOLD_MILLIS);
    }

    public SilentErrorHandler(String description, Logger log, long subscribeRetryWaitTimeMillis, long shoutingThresholdMillis) {
        this.log = log;
        this.sentinelDescription = description;
        this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
        this.shoutingThresholdMillis = shoutingThresholdMillis;
    }

    @Override
    public void handleError(JedisConnectionException exception, boolean running) {
        final long errorTimeMillis = System.currentTimeMillis();
        final long timeElapsedSinceLastErrorMillis = errorTimeMillis - lastErrorTimeMillis;
        lastErrorTimeMillis = errorTimeMillis;
        if (running) {
            logConnectionLoss(timeElapsedSinceLastErrorMillis);
            applySleepPeriod();
        } else {
            log.info("Unsubscribing from " + sentinelDescription);
        }
    }

    //will log as severe if previous connection loss occurred less than shoutingThresholdMillis
    private void logConnectionLoss(long timeElapsedSinceLastErrorMillis) {
        final String msg = "Lost connection to " + sentinelDescription + ". Sleeping 5000ms and retrying.";
        if (timeElapsedSinceLastErrorMillis <= shoutingThresholdMillis) {
            log.severe(msg);
        } else {
            log.fine(msg);
        }
    }

    private void applySleepPeriod() {
        try {
            TimeUnit.MILLISECONDS.sleep(subscribeRetryWaitTimeMillis);
        } catch (InterruptedException e1) {
            log.info("SilentErrorHandler interrupted while sleeping after connection loss");
        }
    }
}
