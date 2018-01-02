package qunar.tc.qmq.web.service.impl;

import qunar.tc.qmq.ListenerHolder;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.consumer.MessageConsumerProvider;
import qunar.tc.qmq.web.service.ConsumerTestService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yunfeng.yang
 * @since 2017/7/12
 */
public class ConsumerTestServiceImpl implements ConsumerTestService {

    private MessageConsumerProvider messageConsumer;
    private ConcurrentMap<String, ListenerHolder> listenerHolders = new ConcurrentHashMap<String, ListenerHolder>();

    public ConsumerTestServiceImpl() {
        this.messageConsumer = new MessageConsumerProvider();
        try {
            this.messageConsumer.afterPropertiesSet();
        } catch (Exception e) {
            throw new RuntimeException("consumer init error");
        }
    }

    // 注册监听
    public void addListener(String subjectPrefix, String consumerGroup, MessageListener messageListener) {
        ListenerHolder listenerHolder = messageConsumer.addListener(subjectPrefix, consumerGroup, messageListener);
        String key = generateKey(subjectPrefix, consumerGroup);
        listenerHolders.putIfAbsent(key, listenerHolder);
    }

    // 注销监听
    public void stopListener(String subjectPrefix, String consumerGroup) {
        String key = generateKey(subjectPrefix, consumerGroup);
        ListenerHolder listenerHolder = listenerHolders.get(key);
        if (listenerHolder != null) {
            listenerHolder.stopListen();
        }
    }

    private String generateKey(String subjectPrefix, String consumerGroup) {
        return subjectPrefix + "??" + consumerGroup;
    }
}
