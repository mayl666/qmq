package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.common.MapKeyBuilder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yiqun.fan create on 17-9-12.
 */
public class PullConsumerFactory {
    private static final ConcurrentMap<String, PullConsumer> pullConsumerMap = new ConcurrentHashMap<>();

    public static PullConsumer getOrCreate(String subject, String group) {
        final String key = MapKeyBuilder.buildSubscribeKey(subject, group);
        PullConsumer consumer = pullConsumerMap.get(key);
        if (consumer != null) {
            return consumer;
        }
        consumer = new PullConsumerImpl(subject, group);
        PullConsumer oldConsumer = pullConsumerMap.putIfAbsent(key, consumer);
        return oldConsumer != null ? oldConsumer : consumer;
    }
}
