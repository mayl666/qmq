/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.consumer.handler;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ListenerHolder;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.RejectPolicy;
import qunar.tc.qmq.ReliabilityLevel;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.ChannelState;
import qunar.tc.qmq.base.QueryRequest;
import qunar.tc.qmq.common.QmqLogger;
import qunar.tc.qmq.common.TracedMessage;
import qunar.tc.qmq.consumer.exception.DuplicateListenerException;
import qunar.tc.qmq.consumer.register.ConsumerRegister;
import qunar.tc.qmq.consumer.register.ExecutorConfig;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.service.ConsumerMessageHandler;
import qunar.tc.qmq.service.exceptions.ConsumerRejectException;
import qunar.tc.qtracer.*;
import qunar.tc.qtracer.impl.QTraceClientGetter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-28
 */
public class MessageDistributor implements ConsumerMessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(MessageDistributor.class);

    /**
     * 注册的消息处理器
     */
    private static final ConcurrentMap<String, MessageHandler> handlers = new ConcurrentHashMap<>();
    /**
     * 接收到的消息
     */
    private final MessageInProcessHolder inProcessHolder;

    private final ConsumerRegister register;

    private String myAddress;

    private String brokerGroupRoot;

    private final QTraceClient tracer;

    public MessageDistributor(ConsumerRegister register) {
        this.register = register;
        this.inProcessHolder = new MessageInProcessHolder();
        this.tracer = QTraceClientGetter.getClient();
    }

    public ListenerHolder addListener(final String subjectPrefix, final String consumerGroup, MessageListener listener, Executor executor, RejectPolicy rejectPolicy) {
        final String realGroup = getRealGroup(consumerGroup);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(realGroup));
        final boolean isBroadcast = !realGroup.equals(consumerGroup);
        if (isBroadcast) {
            logger.info("订阅广播消息, subject:{}, consumer group:{}", subjectPrefix, realGroup);
        } else {
            logger.info("订阅消息, subject:{}, consumer group:{}", subjectPrefix, realGroup);
        }

        final String key = generateKey(subjectPrefix, realGroup);
        final MessageHandler handler = new MessageHandler(executor, listener, inProcessHolder, register, brokerGroupRoot);
        if (handlers.putIfAbsent(key, handler) != null) {
            logger.error("重复注册listener, subject:{}, consumer group:{}", subjectPrefix, consumerGroup);
            throw new DuplicateListenerException(key);
        }

        ExecutorConfig executorConfig = new ExecutorConfig(executor);
        executorConfig.setRejectPolicy(rejectPolicy);
        //如果用户显式设置了zk地址，则即使message里没有zk地址，也可以从register里取
        register.regist(subjectPrefix, realGroup, new RegistParam(executorConfig, listener, isBroadcast));
        return new ListenerHolder() {

            @Override
            public void stopListen() {
                MessageHandler h = handlers.get(key);
                if (h == null || !h.equals(handler)) // 说明被取消过.
                    return;
                h = handlers.remove(key);
                if (h == null || !h.equals(handler)) { // 解决多线程冲突. 虽然极少发生,但是要严谨.
                    handlers.putIfAbsent(key, h);
                    return;
                }
                register.unregist(subjectPrefix, realGroup);
            }
        };
    }

    private String getRealGroup(String consumerGroup) {
        if (Strings.isNullOrEmpty(consumerGroup)) return myAddress;
        return consumerGroup;
    }

    private static String generateKey(String subjectPrefix, String group) {
        return subjectPrefix + "/" + group;
    }

    @Override
    public ChannelState handle(BaseMessage message) throws ConsumerRejectException {
        QTraceScope scope = startQTrace(message);

        String prefix = message.getStringProperty(BaseMessage.keys.qmq_prefix);
        String group = message.getStringProperty(BaseMessage.keys.qmq_consumerGroupName);

        String key = generateKey(prefix, group);

        QmqLogger.log(message, "收到一条消息,此消息会被匹配到: " + key);

        MessageHandler handler = handlers.get(key);
        if (handler == null) {
            String err = "消息匹配失败,没有找到Listener: " + key;
            scope.addTimeAnnotation(err);
            scope.close();
            QmqLogger.log(message, err);
            throw new RuntimeException(err);
        }

        ChannelState cs;
        try {
            cs = handler.handle(message);
            scope.addTimeAnnotation("enter queue");
            QmqLogger.log(message, "进入执行队列");
        } catch (ConsumerRejectException e) {
            scope.addTimeAnnotation("consumer reject");
            QmqLogger.log(message, "消息进入队列失败，请调整消息处理线程池大小");
            throw e;
        } finally {
            scope.close();
        }

        return cs;
    }

    private QTraceScope startQTrace(BaseMessage message) {
        if (message.getBooleanProperty(TracedMessage.QSCHEDULE_DESC)) {
            return MOCK;
        } else {
            String traceId = extractTraceId(message);
            String spanId = message.getStringProperty(BaseMessage.keys.qmq_spanId);
            QTraceScope scope = tracer.startTrace("qmq.consumer", new TraceInfo(traceId, spanId));
            scope.addAnnotation(Constants.QTRACE_TYPE, Constants.QTRACE_TYPE_QMQ);
            scope.addAnnotation("subject", message.getSubject());
            scope.addAnnotation("messageId", message.getMessageId());
            scope.addAnnotation("group", message.getStringProperty(BaseMessage.keys.qmq_consumerGroupName));
            return scope;
        }
    }

    private String extractTraceId(BaseMessage message) {
        boolean low = ReliabilityLevel.isLow(message);
        String traceId = message.getStringProperty(BaseMessage.keys.qmq_traceId);
        boolean nullTraceId = Strings.isNullOrEmpty(traceId);
        return low && nullTraceId ? Constants.DO_NOT_TRACE_TRACEID : traceId;
    }

    @Override
    public List<Integer> queryMessageState(List<QueryRequest> requests) {
        List<Integer> result = new ArrayList<Integer>(requests.size());
        for (int i = 0; i < requests.size(); ++i) {
            QueryRequest request = requests.get(i);
            result.add(inProcessHolder.processState(request));
        }
        return result;
    }

    public void setMyAddress(String myAddress) {
        this.myAddress = myAddress;
    }

    public void setBrokerGroupRoot(String brokerGroupRoot) {
        this.brokerGroupRoot = brokerGroupRoot;
    }

    private static final QScheduleQTraceScope MOCK = new QScheduleQTraceScope();

    private static class QScheduleQTraceScope implements QTraceScope {

        @Override
        public void addAnnotation(String key, String value) {

        }

        @Override
        public void addTimeAnnotation(String msg) {

        }

        @Override
        public void addTraceContext(String key, String value) {

        }

        @Override
        public ImmutableMap<String, String> getTraceContext() {
            return null;
        }

        @Override
        public void close() {

        }

        @Override
        public Span detach() {
            return null;
        }
    }
}
