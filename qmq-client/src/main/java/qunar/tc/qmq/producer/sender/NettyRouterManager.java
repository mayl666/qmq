package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.producer.ConfigCenter;
import qunar.tc.qmq.producer.QueueSender;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhenyu.nie created on 2017 2017/7/3 14:21
 */
class NettyRouterManager extends AbstractRouterManager {

    private final AtomicBoolean STARTED = new AtomicBoolean(false);

    private static final ConfigCenter configs = ConfigCenter.getInstance();

    private QueueSender sender;

    @Override
    public void init() {
        if (STARTED.compareAndSet(false, true)) {
            NettyProducerClient producerClient = new NettyProducerClient();
            producerClient.start();
            setRouter(new NettyRouter(producerClient));
            sender = new RPCQueueSender("dubbo-sender", configs.getMaxQueueSize(), configs.getSendThreads(), configs.getSendBatch(), this);
        }
    }

    @Override
    public String name() {
        return "netty";
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
    }
}
