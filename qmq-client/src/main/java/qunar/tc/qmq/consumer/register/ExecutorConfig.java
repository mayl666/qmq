package qunar.tc.qmq.consumer.register;

import qunar.concurrent.PooledExecutor;
import qunar.tc.qmq.RejectPolicy;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * User: zhaohuiyu
 * Date: 9/18/14
 * Time: 4:25 PM
 */
public class ExecutorConfig {
    public final String queueSize;
    public final String minThread;
    public final String maxThread;
    private Executor executor;

    public ExecutorConfig(Executor executor) {
        this(getQueueCapacity(executor), getMinThreads(executor), getMaxThreads(executor));
        this.executor = executor;
    }

    private ExecutorConfig(int queueSize, int minThread, int maxThread) {
        this.queueSize = String.valueOf(queueSize);
        this.minThread = String.valueOf(minThread);
        this.maxThread = String.valueOf(maxThread);
    }

    public Executor getExecutor() {
        return executor;
    }

    private static int getQueueCapacity(Executor executor) {
        if (executor instanceof PooledExecutor) {
            return ((PooledExecutor) executor).getQueueCapacity();
        }

        if (executor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) executor).getQueue().remainingCapacity();
        }

        return 0;
    }

    private static int getMinThreads(Executor executor) {
        if (executor instanceof PooledExecutor) {
            return ((PooledExecutor) executor).getMinThreads();
        }

        if (executor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) executor).getCorePoolSize();
        }

        return 0;
    }

    private static int getMaxThreads(Executor executor) {
        if (executor instanceof PooledExecutor) {
            return ((PooledExecutor) executor).getMaxThreads();
        }

        if (executor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) executor).getMaximumPoolSize();
        }

        return 0;
    }

    public void setRejectPolicy(RejectPolicy rejectPolicy) {
        if (executor == null) return;
        if (executor instanceof PooledExecutor) {
            ((PooledExecutor) executor).setRejectedExecutionHandler(rejectPolicy.getHandler());
        }

        if (executor instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) executor).setRejectedExecutionHandler(rejectPolicy.getHandler());
        }
    }
}
