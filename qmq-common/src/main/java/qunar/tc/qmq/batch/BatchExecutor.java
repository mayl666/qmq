package qunar.tc.qmq.batch;

import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.*;

/**
 * User: zhaohuiyu Date: 6/4/13 Time: 5:21 PM
 */
public class BatchExecutor<Item> implements Runnable {
    private final String name;
    private final int batchSize;
    private final Processor<Item> processor;

    private static final int DEFAULT_QUEUE_SIZE = 1000;
    private static final int DEFAULT_PROCESS_THREADS = Runtime.getRuntime().availableProcessors() + 1;

    private int queueSize = DEFAULT_QUEUE_SIZE;
    private int threads = DEFAULT_PROCESS_THREADS;

    private BlockingQueue<Item> queue;
    private ThreadPoolExecutor executor;

    public BatchExecutor(String name, int batchSize, Processor<Item> processor) {
        this(name, batchSize, processor, DEFAULT_PROCESS_THREADS);
    }

    public BatchExecutor(String name, int batchSize, Processor<Item> processor, int threads) {
        Preconditions.checkNotNull(processor);

        this.name = name;
        this.batchSize = batchSize;
        this.processor = processor;
        this.threads = threads;
    }

    @PostConstruct
    public void init() {
        this.queue = new LinkedBlockingQueue<Item>(this.queueSize);
        if (this.executor == null) {
            this.executor = new ThreadPoolExecutor(1, threads, 1L, TimeUnit.MINUTES,
                    new ArrayBlockingQueue<Runnable>(1), new NamedThreadFactory("batch-" + name + "-task", true));
            executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        }
    }

    public boolean addItem(Item item) {
        boolean offer = this.queue.offer(item);
        if (offer) {
            this.executor.execute(this);
        }
        return offer;
    }

    public boolean addItem(Item item, long timeout, TimeUnit unit) throws InterruptedException {
        boolean offer = this.queue.offer(item, timeout, unit);
        if (offer) {
            this.executor.execute(this);
        }
        return offer;
    }

    @Override
    public void run() {
        while (!this.queue.isEmpty()) {
            List<Item> list = Lists.newArrayListWithCapacity(batchSize);
            int size = this.queue.drainTo(list, batchSize);
            if (size > 0) {
                this.processor.process(list);
            }
        }
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public void setExecutor(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    @PreDestroy
    public void destroy() {
        if (executor != null) {
            executor.shutdown();
        }
    }
}
