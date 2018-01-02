package qunar.tc.qmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.Map;

/**
 * @author yiqun.fan create on 17-8-19.
 */
public class ConsumeMessage extends BaseMessage {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeMessage.class);
    private static final int MAX_MESSAGE_RETRY_THRESHOLD = 5;

    private transient final Thread processThread;
    private volatile boolean autoAck = true;
    private volatile transient int localRetries;
    private volatile Map<String, Object> filterContext;
    private volatile int processedFilterIndex = -1;

    protected ConsumeMessage(BaseMessage message) {
        super(message);
        this.processThread = Thread.currentThread();
        int times = message.times();
        if (times > MAX_MESSAGE_RETRY_THRESHOLD) {
            LOGGER.warn("这是第 {} 次收到同一条消息，请注意检查逻辑是否有问题. subject={}, msgId={}",
                    times, RetrySubjectUtils.getRealSubject(message.getSubject()), message.getMessageId());
        }
    }

    boolean isAutoAck() {
        return this.autoAck;
    }

    @Override
    public void autoAck(boolean auto) {
        if (processThread != Thread.currentThread()) {
            throw new RuntimeException("如需要使用显式ack，请在MessageListener的onMessage入口处调用autoAck设置，也不能在其他线程里调用");
        }
        this.autoAck = auto;
    }

    public Map<String, Object> getFilterContext() {
        return filterContext;
    }

    void setFilterContext(Map<String, Object> filterContext) {
        this.filterContext = filterContext;
    }

    int getProcessedFilterIndex() {
        return processedFilterIndex;
    }

    void setProcessedFilterIndex(int processedFilterIndex) {
        this.processedFilterIndex = processedFilterIndex;
    }

    @Override
    public int localRetries() {
        return localRetries;
    }

    void localRetries(int localRetries) {
        this.localRetries = localRetries;
    }
}
