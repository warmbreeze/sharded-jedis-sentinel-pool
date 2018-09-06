package redis.clients.jedis.errors;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.WaitStrategies;
import com.github.rholder.retry.WaitStrategy;

import redis.clients.jedis.MasterListenerErrorHandler;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Created by piotrturek on 30/03/15.
 */
public class SilentErrorHandler implements MasterListenerErrorHandler {
    private final Logger log;
    private static final long DEFAULT_SUBSCRIBE_RETRY_WAIT_TIME_MILLIS = 5000;
    private static final long DEFAULT_SHOUTING_THRESHOLD_MILLIS = 20000;
    private String sentinelDescription;
    private long shoutingThresholdMillis;
    private long lastErrorTimeMillis = -1;
    
    private AttemptTracker attemptTracker = new AttemptTracker();
    private WaitStrategy waitStrategy;

    public SilentErrorHandler(String description, Logger log) {
        this(description, log, DEFAULT_SUBSCRIBE_RETRY_WAIT_TIME_MILLIS, DEFAULT_SHOUTING_THRESHOLD_MILLIS);
    }

    public SilentErrorHandler(String description, Logger log, long subscribeRetryWaitTimeMillis, long shoutingThresholdMillis) {
        this.log = log;
        this.sentinelDescription = description;
        this.shoutingThresholdMillis = shoutingThresholdMillis;
        this.waitStrategy = WaitStrategies.exponentialWait(subscribeRetryWaitTimeMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void handleError(JedisConnectionException exception, boolean running) {
        long errorTimeMillis = System.nanoTime() / 1_000_000;
        long timeElapsedSinceLastErrorMillis = errorTimeMillis - lastErrorTimeMillis;
        lastErrorTimeMillis = errorTimeMillis;
        boolean realError = isRealError(timeElapsedSinceLastErrorMillis);
        if (running) {
            logConnectionLoss(realError);
            applySleepPeriod(realError);
        } else {
            log.info("Unsubscribing from " + sentinelDescription);
        }
    }

    //will log as severe if previous connection loss occurred less than shoutingThresholdMillis
    private void logConnectionLoss(boolean realError) {
        String msg = "Lost connection to " + sentinelDescription + ". Sleeping 5000ms and retrying.";
        if (realError) {
            log.severe(msg);
        } else {
            log.fine(msg);
        }
    }

    private boolean isRealError(long timeElapsedSinceLastErrorMillis) {
        return timeElapsedSinceLastErrorMillis <= shoutingThresholdMillis;
    }

    private void applySleepPeriod(boolean realError) {
        if (realError) {
            attemptTracker.markFailedAttempt();
        } else {
            attemptTracker.markSuccessfulAttempt();
        }
        
        long sleepTimeMs = waitStrategy.computeSleepTime(attemptTracker);
        try {
            System.out.println("sleeping " + sleepTimeMs + "ms");
            TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
        } catch (InterruptedException e1) {
            log.info("SilentErrorHandler interrupted while sleeping after connection loss");
        }
    }
    
    static class AttemptTracker implements Attempt<Void> {
        private static final int INITIAL_ATTEMPT_NUMBER = 1;
        private int attemptNumber = INITIAL_ATTEMPT_NUMBER;

        public void markFailedAttempt() {
            attemptNumber++;
        }

        public void markSuccessfulAttempt() {
            attemptNumber = INITIAL_ATTEMPT_NUMBER;
        }

        @Override
        public Void get() throws ExecutionException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasResult() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasException() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void getResult() throws IllegalStateException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Throwable getExceptionCause() throws IllegalStateException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getAttemptNumber() {
            return attemptNumber;
        }

        @Override
        public long getDelaySinceFirstAttempt() {
            throw new UnsupportedOperationException();
        }
    }
}
