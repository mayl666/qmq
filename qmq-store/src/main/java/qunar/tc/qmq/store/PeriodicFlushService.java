package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author keli.wang
 * @since 2017/7/7
 */
public class PeriodicFlushService {
    private static final Logger LOG = LoggerFactory.getLogger(PeriodicFlushService.class);

    private final FlushProvider flushProvider;
    private final Thread thread;
    private volatile boolean stopped = false;

    public PeriodicFlushService(final FlushProvider flushProvider) {
        this.flushProvider = flushProvider;
        this.thread = new Thread(new FlushRunnable());
    }

    public boolean isStopped() {
        return stopped;
    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        stopped = true;

        try {
            thread.interrupt();
            if (!thread.isDaemon()) {
                thread.join(flushProvider.getJoinTime());
            }
        } catch (InterruptedException e) {
            LOG.warn("shutdown flush thread interrupted.");
        }
    }

    public interface FlushProvider {
        int getJoinTime();

        int getInterval();

        void flush();
    }

    private class FlushRunnable implements Runnable {
        @Override
        public void run() {
            while (!isStopped()) {
                try {
                    Thread.sleep(flushProvider.getInterval());
                    flushProvider.flush();
                } catch (InterruptedException ignore) {
                    LOG.info("Flush thread interrupted. Will flush one more time before exit.");
                } catch (Exception e) {
                    LOG.error("flush service has exception.", e);
                }
            }

            flushProvider.flush();
        }
    }
}
