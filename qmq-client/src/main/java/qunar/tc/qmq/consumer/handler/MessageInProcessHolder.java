package qunar.tc.qmq.consumer.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Numbers;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.MessageProcessState;
import qunar.tc.qmq.base.QueryRequest;
import qunar.tc.qmq.utils.PropertiesLoader;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import static qunar.tc.qmq.ReliabilityLevel.isLow;

/**
 * User: zhaohuiyu
 * Date: 3/21/14
 * Time: 2:05 PM
 */
class MessageInProcessHolder {
    private static final Logger logger = LoggerFactory.getLogger(MessageInProcessHolder.class);

    private static final int MEMORY_LEAK_WARNING_COUNT;

    static {
        Properties parent = System.getProperties();
        Properties bundle = PropertiesLoader.load("qmq-consumer.properties", parent);

        if (bundle == null) {
            bundle = new Properties(parent);
        }

        MEMORY_LEAK_WARNING_COUNT = Numbers.toInt(bundle.getProperty("memoryleak.warningcount"), 10000);
    }

    /**
     * 正在处理的消息
     */
    private final ConcurrentMap<String, Future<?>> messageInProcess;

    MessageInProcessHolder() {
        this.messageInProcess = new ConcurrentHashMap<String, Future<?>>();
    }

    boolean enqueue(BaseMessage message, Future task) {
        //如果是非可靠消息，不用记录这个
        if (isLow(message)) return true;

        return messageInProcess.putIfAbsent(generateIdentity(message), task) == null;
    }

    void dequeue(BaseMessage message) {
        //如果任务被拒绝，则需要显式的删除记录，避免内存泄露
        if (!isLow(message)) {
            Future<?> removed = messageInProcess.remove(generateIdentity(message));
            if (removed == null) {
                logger.error("Can not removed anything in message in process, maybe a memory leak");
            }
        }
    }

    int processState(QueryRequest request) {
        if (messageInProcess.size() >= MEMORY_LEAK_WARNING_COUNT) {
            logger.error("Maybe a memory leak", new RuntimeException("WARNING! memory leak"));
        }
        Future<?> future = messageInProcess.get(generateIdentity(request));
        if (future == null) {
            return MessageProcessState.FAILED;
        } else if (future.isDone()) {
            return MessageProcessState.COMPLETED;
        } else {
            return MessageProcessState.PROCESSING;
        }
    }

    private static String generateIdentity(QueryRequest request) {
        return generateIdentity(request.getPrefix(), request.getGroup(), request.getMessageId());
    }

    private static String generateIdentity(Message message) {
        final String id = message.getMessageId();
        final String prefix = message.getStringProperty(BaseMessage.keys.qmq_prefix.name());
        final String group = message.getStringProperty(BaseMessage.keys.qmq_consumerGroupName.name());
        return generateIdentity(prefix, group, id);
    }

    private static String generateIdentity(String prefix, String group, String messageId) {
        return prefix + "$" + group + "$" + messageId;
    }
}
