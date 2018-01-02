/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import qunar.agile.Dates;
import qunar.tc.qmq.*;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.common.QmqLogger;
import qunar.tc.qmq.common.TracedMessage;
import qunar.tc.qmq.producer.idgenerator.IdGenerator;
import qunar.tc.qmq.producer.idgenerator.TimestampAndHostIdGenerator;
import qunar.tc.qmq.producer.sender.*;
import qunar.tc.qmq.producer.tx.MessageTracker;
import qunar.tc.qmq.serializer.Serializer;
import qunar.tc.qmq.serializer.SerializerFactory;
import qunar.tc.qmq.utils.Applications;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static qunar.tc.qmq.config.QConfigRegistryResolver.INSTANCE;
import static qunar.tc.qmq.utils.Constants.DEFAULT_GROUP;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-5
 */
public class MessageProducerProvider implements MessageProducer, InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(MessageProducerProvider.class);

    private static final ConfigCenter configs = ConfigCenter.getInstance();

    private final IdGenerator idGenerator;

    private final Serializer serializer;

    private MessageSendStateListener globalListener;

    private final Applications applications = Applications.getInstance();

    private final AtomicBoolean STARTED = new AtomicBoolean(false);

    private final RouterManagerDispatcher routerManagerDispatcher;

    private final ReliabilityLevelResolver reliabilityLevelResolver;

    private final QTracerConfig qTracerConfig;

    private MessageTracker messageTracker;

    /**
     * 自动路由机房
     */
    public MessageProducerProvider() {
        this(new DefaultRouterManagerDispatcher(new DubboRouterManager(new QConfigRouter(INSTANCE))));
    }
    /**
     * 显式设置发送机房
     *
     * @param registryURL
     */
    public MessageProducerProvider(String registryURL) {
        this(new DefaultRouterManagerDispatcher(new DubboRouterManager(new LocalConfigRouter(registryURL, DEFAULT_GROUP))));
    }

    /**
     * 显示设置机房
     *
     * @param registryURL
     * @param group
     */
    public MessageProducerProvider(String registryURL, String group) {
        this(new DefaultRouterManagerDispatcher(new DubboRouterManager(new LocalConfigRouter(registryURL, group))));
    }

    private MessageProducerProvider(DefaultRouterManagerDispatcher routerManagerDispatcher) {
        this.routerManagerDispatcher = routerManagerDispatcher;
        this.idGenerator = new TimestampAndHostIdGenerator();
        this.serializer = SerializerFactory.create();
        this.reliabilityLevelResolver = new ReliabilityLevelResolver();
        this.qTracerConfig = new QTracerConfig();
    }

    /**
     * 已经实现了InitializingBean，无需再配置init方法
     */
    @Deprecated
    public void init() {
        if (STARTED.compareAndSet(false, true)) {
            routerManagerDispatcher.init();
        }
    }
    /*指定主题*/

    @Override
    public Message generateMessage(String subject) {
        return generateMessage(subject, configs.getMinExpiredTime(), TimeUnit.MINUTES);
    }
/*指定消息唯一键和主题*/
    @Override
    public Message generateMessage(String messageId, String subject) {
        return generateMessage(subject + messageId, subject, configs.getMinExpiredTime(), TimeUnit.MINUTES);
    }

    @Override
    public Message generateMessage(String subject, long expire, TimeUnit timeUnit) {
        return generateMessage(idGenerator.getNext(), subject, expire, timeUnit);
    }

    @Override
    public Message generateMessage(String messageId, String subject, long expire, TimeUnit timeUnit) {
        validateExpiredTime(expire, timeUnit);
        BaseMessage msg = new BaseMessage(messageId, subject);
        msg.setSerializer(serializer);

        msg.setExpiredDelay(expire, timeUnit);
        msg.setProperty(BaseMessage.keys.qmq_appCode, applications.getAppCode());
        return msg;
    }

    private void validateExpiredTime(long timeDelay, TimeUnit timeUnit) {
        long time = timeUnit.toMillis(timeDelay);
        if (time < configs.getMinExpiredTime() * Dates.MINUTE) {
            throw new RuntimeException("消息过期时间不能为过去时");
        }
        if (time > Dates.DAY) {
            throw new RuntimeException("消息的过期时间不能超过24小时");
        }
    }

    @Override
    public void sendMessage(Message message) {
        sendMessage(message, globalListener);
    }

    @Override
    public void sendMessage(Message message, MessageSendStateListener listener) {
        if (!STARTED.get()) {
            throw new RuntimeException("MessageProducerProvider未初始化，如果使用非Spring的方式请确认afterPropertiesSet是否调用");
        }

        ProduceMessageImpl pm = initProduceMessage(message, listener);

        QmqLogger.log(pm.getBase(), "准备发送");

        if (messageTracker == null) {
            message.setDurable(false);
        }

        if (!message.isDurable()) {
            pm.addAnnotation("level", "non-durable");
            validateMessage(pm);
            QmqLogger.log(pm.getBase(), "非持久消息");
            pm.send();
            return;
        }

        if (!messageTracker.trackInTransaction(pm)) {
            pm.send();
        }
    }

    private void validateMessage(ProduceMessageImpl message) {
        if (ReliabilityLevel.isLow(message.getBase())) return;

        String content = serializer.serialize(message.getBase());
        int length = content.length();
        if (length > configs.getMaxMessageSize()) {
            String messageId = message.getMessageId();
            message.addAnnotation(qunar.tc.qtracer.Constants.EXCEPTION_KEY, "too big message " + length);
            log.error("消息长度: {}, 超过指定大小: {}, messageId: {} ", length, configs.getMaxMessageSize(), messageId);
            message.closeTrace();
            throw new RuntimeException("消息长度超过指定大小, message id: " + messageId);
        }
    }

    private ProduceMessageImpl initProduceMessage(Message message, MessageSendStateListener listener) {
        BaseMessage base = (BaseMessage) message;
        TracedMessage tracedMessage = TracedMessage.producer(base, qTracerConfig);
        RouterManager routerManager = routerManagerDispatcher.dispatch(base);
        ProduceMessageImpl pm = new ProduceMessageImpl(tracedMessage, routerManager.getSender());
        pm.setSendTryCount(configs.getSendTryCount());
        pm.setSendStateListener(listener);
        pm.setSyncSend(configs.isSyncSend());

        pm.getBase().setProperty(BaseMessage.keys.qmq_registry, routerManager.registryOf(message));

        configReliabilityLevel(message);
        return pm;
    }

    private void configReliabilityLevel(Message message) {
        message.setReliabilityLevel(reliabilityLevelResolver.resolve(message));
    }

    /**
     * !!!!已经标记为过期，不推荐使用，会在两个版本后删除 请使用sendMessage的第二个参数设置listener
     *
     * @param listener
     */
    @Deprecated
    public void setMessageSendStateListener(MessageSendStateListener listener) {
        log.error("方法已过期", new RuntimeException("setMessageSendStateListener已经标记为过期，请使用sendMessage方法传递该listener, eg: producer.sendMessage(message, listener);"));
        this.globalListener = listener;
    }

    @Override
    public void destroy() {
        routerManagerDispatcher.destroy();
    }

    /**
     * 内存发送队列最大值，默认值 @see QUEUE_MEM_SIZE
     *
     * @param maxQueueSize 内存队列大小
     */
    public void setMaxQueueSize(int maxQueueSize) {
        configs.setMaxQueueSize(maxQueueSize);
    }

    /**
     * 发送线程数 @see SEND_THREADS
     *
     * @param sendThreads
     */
    public void setSendThreads(int sendThreads) {
        configs.setSendThreads(sendThreads);
    }

    /**
     * 批量发送，每批量大小 @see SEND_BATCH
     *
     * @param sendBatch
     */
    public void setSendBatch(int sendBatch) {
        configs.setSendBatch(sendBatch);
    }

    /**
     * 发送失败重试次数
     *
     * @param sendTryCount @see SEND_TRY_COUNT
     */
    public void setSendTryCount(int sendTryCount) {
        configs.setSendTryCount(sendTryCount);
    }

    public void setTransactionProvider(TransactionProvider transactionProvider) {
        this.messageTracker = new MessageTracker(transactionProvider);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }
}
