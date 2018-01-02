package qunar.tc.qmq.consumer;

import qunar.concurrent.NamedThreadFactory;
import qunar.metrics.Gauge;
import qunar.metrics.Metrics;
import qunar.tc.qmq.common.LimitedBlockingQueueWrapper;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yiqun.fan create on 17-8-21.
 */
class ThreadPoolExecutorManager {
    private static final int DEFAULT_CORE_THREADS = 1;
    private static final int DEFAULT_MAX_THREADS = 2 * Runtime.getRuntime().availableProcessors();
    private static final int DEFAULT_QUEUE_SIZE = 1000;
    private static final int DEFAULT_KEEP_TIME_MINS = 1;
    private static final RejectedExecutionHandler DEFAULT_REJECTED_EXECUTION_HANDLER = new ThreadPoolExecutor.AbortPolicy();

    private final String threadPoolNamePrefix;
    private final ConcurrentMap<String, ThreadPoolExecutor> executorMap = new ConcurrentHashMap<>();

    private final AtomicReference<ExecutorConfig> defaultConfig = new AtomicReference<>(ExecutorConfig.DEFAULT);
    private final ConcurrentMap<String, ExecutorConfig> configMap = new ConcurrentHashMap<>();

    ThreadPoolExecutorManager(String threadPoolNamePrefix) {
        this.threadPoolNamePrefix = threadPoolNamePrefix;
    }

    synchronized void setDefaultExecutorConfig(ExecutorConfig config) {
        defaultConfig.set(config);
    }

    ExecutorConfig getDefaultConfig() {
        return defaultConfig.get();
    }

    synchronized void setAndUpdateExecutorConfig(String key, ExecutorConfig config) {
        if (config != null) {
            configMap.put(key, config);
            ThreadPoolExecutor executor = executorMap.get(key);
            if (executor != null) {
                executor.setCorePoolSize(config.coreSize);
                executor.setMaximumPoolSize(config.maxSize);
                if (executor.getQueue() instanceof LimitedBlockingQueueWrapper) {
                    ((LimitedBlockingQueueWrapper) executor.getQueue()).setLimit(config.queueSize);
                }
            }
        }
    }

    private ExecutorConfig getExecutorConfig(String key) {
        ExecutorConfig config = configMap.get(key);
        return config != null ? config : defaultConfig.get();
    }

    synchronized ThreadPoolExecutor getExecutor(String key) {
        ExecutorConfig config = getExecutorConfig(key);
        ThreadPoolExecutor executor = executorMap.get(key);
        if (executor == null) {
            final LimitedBlockingQueueWrapper<Runnable> queue = new LimitedBlockingQueueWrapper<>(config.queueSize);
            executor = new ThreadPoolExecutor(
                    config.coreSize, config.maxSize,
                    DEFAULT_KEEP_TIME_MINS, TimeUnit.MINUTES,
                    queue,
                    createThreadFactory(key),
                    DEFAULT_REJECTED_EXECUTION_HANDLER);
            Metrics.gauge("qmq_pull_default_buffer_size").tag("key", key).call(new Gauge() {
                @Override
                public double getValue() {
                    return queue.size();
                }
            });
            Metrics.gauge("qmq_pull_default_buffer_limit").tag("key", key).call(new Gauge() {
                @Override
                public double getValue() {
                    return queue.getLimit();
                }
            });
            executorMap.put(key, executor);
        }
        return executor;
    }

    private ThreadFactory createThreadFactory(String key) {
        return new NamedThreadFactory(threadPoolNamePrefix + key);
    }

    static final class ExecutorConfig {
        static final ExecutorConfig DEFAULT = new ExecutorConfig(DEFAULT_CORE_THREADS, DEFAULT_MAX_THREADS, DEFAULT_QUEUE_SIZE);

        final int coreSize;
        final int maxSize;
        final int queueSize;

        ExecutorConfig(int coreSize, int maxSize, int queueSize) {
            this.coreSize = coreSize;
            this.maxSize = maxSize;
            this.queueSize = queueSize;
        }
    }
}
