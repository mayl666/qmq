package qunar.tc.qmq.batch;

import qunar.concurrent.NamedThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by zhaohui.yu
 * 16/9/14
 */
public class ExecutorUtil {
    public static final ExecutorService SHARED = Executors.newCachedThreadPool(new NamedThreadFactory("qmq"));

    public static final ScheduledExecutorService SHARED_SCHEDULED = Executors.newScheduledThreadPool(5, new NamedThreadFactory("qmq-sched"));
}
