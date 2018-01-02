package qunar.tc.qmq.batch;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * User: zhaohuiyu
 * Date: 6/3/14
 * Time: 3:11 PM
 */
public class Flusher<Item> {
    private static final Logger logger = LoggerFactory.getLogger(Flusher.class);

    private final FlushThread<Item>[] flushers;

    private final Future[] futures;

    private int flushInterval;

    private AtomicInteger index;

    private static final Random r = new Random();

    private static final int delta = 50;

    public Flusher(String name, int bufferSize, int flushInterval, int queueSize, int threads, Processor<Item> processor) {
        this.flushers = new FlushThread[threads];
        this.futures = new Future[threads];
        this.flushInterval = flushInterval;

        if (threads > 1) index = new AtomicInteger();

        for (int i = 0; i < threads; ++i) {
            final FlushThread<Item> flushThread = new FlushThread<Item>(name + "-" + i, bufferSize, flushInterval, queueSize, processor);
            flushers[i] = flushThread;
            ExecutorUtil.SHARED.submit(flushThread);
            futures[i] = ExecutorUtil.SHARED_SCHEDULED.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    flushThread.timeout();
                }
            }, r.nextInt(delta), flushInterval, TimeUnit.MILLISECONDS);
        }
    }
    
    public boolean add(Item item) {
        int len = flushers.length;
        if (len == 1) return flushers[0].add(item);
        return flushers[Math.abs(index.incrementAndGet() % len)].add(item);
    }

    public void setFlushInterval(int flushInterval) {
        Preconditions.checkArgument(flushInterval > 0);

        if (this.flushInterval == flushInterval) return;
        this.flushInterval = flushInterval;
        for (int i = 0; i < futures.length; ++i) {
            futures[i].cancel(false);
            final FlushThread<Item> flushThread = flushers[i];
            flushThread.setFlushInterval(flushInterval);
            futures[i] = ExecutorUtil.SHARED_SCHEDULED.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    flushThread.timeout();
                }
            }, r.nextInt(delta), flushInterval, TimeUnit.MILLISECONDS);
        }

    }

    private static class FlushThread<Item> implements Runnable {
        private final String name;
        private final int bufferSize;
        private int flushInterval;
        private final Processor<Item> processor;
        private final MpscLinkedQueue<Item> queue;

        private volatile Thread waiter;

        private volatile long lastFlush;

        public FlushThread(String name, int bufferSize, int flushInterval, int queueSize, Processor<Item> processor) {
            this.name = name;
            this.bufferSize = bufferSize;
            this.flushInterval = flushInterval;
            this.processor = processor;
            this.queue = new MpscLinkedQueue<Item>(queueSize);
        }

        public boolean add(Item item) {
            boolean entered = this.queue.offer(item);
            flushOnDemand();
            return entered;
        }

        private void flushOnDemand() {
            if (queue.SIZE() < bufferSize) return;
            start();
        }

        private void start() {
            LockSupport.unpark(waiter);
        }

        @Override
        public void run() {
            Thread.currentThread().setName(name);
            waiter = Thread.currentThread();

            while (!Thread.currentThread().isInterrupted()) {
                while (!canFlush()) {
                    LockSupport.park(this);
                }
                flush();
            }
        }

        private boolean canFlush() {
            return queue.SIZE() >= bufferSize || System.currentTimeMillis() - lastFlush >= flushInterval;
        }

        private void flush() {
            lastFlush = System.currentTimeMillis();
            List<Item> temp = new ArrayList<Item>(bufferSize);
            int size = this.queue.drainTo(temp, bufferSize);
            if (size > 0) {
                try {
                    this.processor.process(temp);
                } catch (Throwable e) {
                    logger.error("process error", e);
                }
            }
        }

        public void timeout() {
            if (System.currentTimeMillis() - lastFlush < flushInterval) return;
            start();
        }

        public void setFlushInterval(int flushInterval) {
            this.flushInterval = flushInterval;
        }
    }
}
