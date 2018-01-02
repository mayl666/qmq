package qunar.tc.qmq.batch;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.metrics.Gauge;
import qunar.metrics.Metrics;

/**
 * Created by zhaohui.yu
 * 15/9/5
 */
public class MultipleDispatcher<T> {
    private static final Logger logger = LoggerFactory.getLogger(MultipleDispatcher.class);

    private static final int DEFAULT_QUEUE_SIZE = 3000;

    private final ConcurrentMap<String, Dispatcher<T>> dispatchers;
    private final Processor<T> processor;
    private final Executor executor;
    private final AtomicInteger queues;
    private final String prefix;

    public MultipleDispatcher(String prefix, Processor<T> processor) {
        this.prefix = prefix;
        this.processor = processor;
        this.queues = new AtomicInteger();
        this.executor = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        this.dispatchers = Maps.newConcurrentMap();
        Metrics.gauge("dispatchers-" + prefix).keep().call(new Gauge() {
            @Override
            public double getValue() {
                return queues.get();
            }
        });
    }

    public boolean dispatch(String channel, T msg) {
        return dispatch(channel, msg, DEFAULT_QUEUE_SIZE);
    }

    public boolean dispatch(String channel, T msg, int queueSize) {
        Dispatcher<T> dispatcher = createOrGet(channel, queueSize);
        boolean dispatch = dispatcher.dispatch(msg);
        schedule(dispatcher, true);
        return dispatch;
    }

    private Dispatcher<T> createOrGet(String name, int queueSize) {
        Dispatcher<T> dispatcher = dispatchers.get(name);
        if (dispatcher != null) return dispatcher;

        Dispatcher<T> add = new Dispatcher<T>(prefix, name, this, this.processor, 100, 20, queueSize);
        Dispatcher<T> old = dispatchers.putIfAbsent(name, add);
        if (old == null) {
            logger.info("create dispatcher: {}", name);
            queues.incrementAndGet();
            return add;
        }
        return old;
    }

    private void schedule(Dispatcher<T> dispatcher, boolean hasMessageHint) {
        if (!dispatcher.canBeSchedule(hasMessageHint)) return;
        if (dispatcher.setAsSchedule()) {
            this.executor.execute(dispatcher);
        }
    }

    static class Dispatcher<T> extends ForkJoinTask implements Runnable {

        private static final int ONECE = 5;

        private final AtomicBoolean status = new AtomicBoolean(false);

        private final String prefix;
        private final String name;
        private final MultipleDispatcher<T> dispatcher;
        private final BlockingQueue<T> queue;
        private final Processor<T> processor;
        private final int batch;
        private final int timeout;

        public Dispatcher(String prefix, String name, MultipleDispatcher<T> dispatcher, Processor<T> processor, int batch, int timeout, int queueSize) {
            this.prefix = prefix;
            this.name = name;
            this.dispatcher = dispatcher;
            this.processor = processor;
            this.batch = batch;
            this.timeout = timeout;
            this.queue = new LinkedBlockingQueue<T>(queueSize);
        }


        public boolean dispatch(T message) {
            return this.queue.offer(message);
        }

        @Override
        public void run() {
            String old = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(prefix + "-" + name);
                processMessages(batch / ONECE, System.currentTimeMillis() + this.timeout);
            } finally {
                Thread.currentThread().setName(old);
                setAsIdle();
                this.dispatcher.schedule(this, false);
            }
        }

        private void processMessages(int left, long deadline) {
            while (true) {
                for (int i = 0; i < ONECE; ++i) {
                    T message = queue.poll();
                    if (message == null) return;
                    processor.process(message);
                }

                if (left > 1 && (System.currentTimeMillis() - deadline) < 0) {
                    left = left - 1;
                    continue;
                }
                return;
            }
        }

        @Override
        public Object getRawResult() {
            return null;
        }

        @Override
        protected void setRawResult(Object value) {

        }

        @Override
        protected boolean exec() {
            run();
            return false;
        }

        public boolean setAsSchedule() {
            return status.compareAndSet(false, true);
        }

        private boolean setAsIdle() {
            return status.compareAndSet(true, false) || setAsIdle();
        }

        private boolean canBeSchedule(boolean hasMessageHint) {
            return hasMessageHint || !queue.isEmpty();
        }
    }

    public static interface Processor<T> {
        void process(T message);
    }

}
