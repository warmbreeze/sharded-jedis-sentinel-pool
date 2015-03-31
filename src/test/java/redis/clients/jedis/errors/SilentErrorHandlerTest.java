package redis.clients.jedis.errors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.verification.Times;
import org.mockito.runners.MockitoJUnitRunner;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SilentErrorHandlerTest {
    private static final long SHOUTING_THRESHOLD_MILLIS = 500;
    private static final long RETRY_WAIT_MILLIS = 200;

    private SilentErrorHandler instance;
    @Mock
    private Logger log;

    @Before
    public void setUp() throws Exception {
        instance = new SilentErrorHandler("sentinel man", log, RETRY_WAIT_MILLIS, SHOUTING_THRESHOLD_MILLIS);
        doCallRealMethod().when(log).fine(anyString());
        doCallRealMethod().when(log).severe(anyString());
    }

    @Test
    public void handleErrorShouldNotLogSevereUponFirstFailure() throws Exception {
        //when
        instance.handleError(new JedisConnectionException(""), true);

        //then
        verify(log).fine(anyString());
    }

    @Test
    public void handleErrorShouldLogUnsubscribingFromSentinelWhenNotRunning() throws Exception {
        //when
        instance.handleError(new JedisConnectionException(""), false);

        //then
        verify(log).info(anyString());
    }

    @Test
    public void handleErrorShouldLogSevereWhenFailingAfterLessThanThreshold() throws Exception {
        //when
        instance.handleError(new JedisConnectionException(""), true);
        instance.handleError(new JedisConnectionException(""), true);

        //then
        verify(log).fine(anyString());
        verify(log).severe(anyString());
    }

    @Test
    public void handleErrorShouldLogFineWhenFailingAfterMoreThanThreshold() throws Exception {
        //when
        instance.handleError(new JedisConnectionException(""), true);
        TimeUnit.SECONDS.sleep(2);
        instance.handleError(new JedisConnectionException(""), true);

        //then
        verify(log, new Times(2)).fine(anyString());
    }
}