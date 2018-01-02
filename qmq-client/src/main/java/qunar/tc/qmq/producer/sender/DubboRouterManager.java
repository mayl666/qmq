package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.producer.ConfigCenter;
import qunar.tc.qmq.producer.QueueSender;

import java.util.concurrent.atomic.AtomicBoolean;

public class DubboRouterManager extends AbstractRouterManager {

    private final AtomicBoolean STARTED = new AtomicBoolean(false);

    private static final ConfigCenter configs = ConfigCenter.getInstance();

    private QueueSender sender;

    public DubboRouterManager(Router router) {
        setRouter(router);
    }

    @Override
    public void init() {
        if (STARTED.compareAndSet(false, true)) {
            sender = new RPCQueueSender("dubbo-sender", configs.getMaxQueueSize(), configs.getSendThreads(), configs.getSendBatch(), this);
        }
    }

    @Override
    public String name() {
        return "dubbo";
    }

    @Override
    public QueueSender getSender() {
        return sender;
    }

    @Override
    public void destroy() {
        if (sender != null) {
            sender.destroy();
        }

        DubboRoute.destroy();
    }
}
