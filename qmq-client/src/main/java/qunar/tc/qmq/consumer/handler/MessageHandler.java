/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.consumer.handler;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.concurrent.PooledExecutor;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.NeedRetryException;
import qunar.tc.qmq.ReliabilityLevel;
import qunar.tc.qmq.base.ACKMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.ChannelState;
import qunar.tc.qmq.consumer.BaseMessageHandler;
import qunar.tc.qmq.consumer.ConsumeMessage;
import qunar.tc.qmq.consumer.register.ConsumerRegister;
import qunar.tc.qmq.service.exceptions.ConsumerRejectException;
import qunar.tc.qmq.utils.URLUtils;
import qunar.tc.qtracer.Constants;
import qunar.tc.qtracer.QTraceScope;
import qunar.tc.qtracer.QTracer;

import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-28
 */
class MessageHandler extends BaseMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandler.class);

    private final ConsumerRegister register;
    private final String brokerGroupRoot;

    /**
     * 正在处理的消息
     */
    private final MessageInProcessHolder inProcessHolder;

    MessageHandler(Executor executor, MessageListener listener, MessageInProcessHolder inProcessHolder, ConsumerRegister register, String brokerGroupRoot) {
        super(executor, listener);
        this.register = register;
        this.brokerGroupRoot = brokerGroupRoot;
        this.inProcessHolder = inProcessHolder;
    }

    ChannelState handle(final BaseMessage message) throws ConsumerRejectException {
        final ConsumerMessage consumerMessage = new ConsumerMessage(message, MessageHandler.this);

        @SuppressWarnings("unchecked") final FutureTask task = new FutureTask(new HandleTask(consumerMessage, this) {
            @Override
            protected void doAck(long elapsed, Throwable exception) {
                ack(message, elapsed, exception);
            }
        }, null);

        //队列里有重复消息，无法进入
        if (!inProcessHolder.enqueue(message, task)) {
            return new ChannelState(getActiveCount(executor), 0);
        }

        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            inProcessHolder.dequeue(message);
            throw new ConsumerRejectException("客户端线程已满: " + getActiveCount(executor));
        }
        return new ChannelState(getActiveCount(executor), 0);
    }

    void sendAck(ConsumerMessage message, long elapsed, Throwable ex) {
        applyPostOnMessage(message, ex, new HashMap<>(message.getFilterContext()));
        ack(message, elapsed, ex);
    }

    private void ack(ConsumeMessage message, long elapsed, Throwable exception) {
        printError(message, exception);
        if (ReliabilityLevel.isLow(message)) {
            QTracer.addTimelineAnnotation("LowReliabilityLevel");
            return;
        }

        QTraceScope scope = startAckTrace(message);
        final String id = message.getMessageId();
        final String prefix = message.getStringProperty(BaseMessage.keys.qmq_prefix);
        final String consumerGroup = message.getStringProperty(BaseMessage.keys.qmq_consumerGroupName);

        String errMessage = ((exception == null) ? null : exception.toString());
        ACKMessage ackMessage = new ACKMessage(id, prefix, consumerGroup, elapsed, errMessage);
        if (exception != null && exception instanceof NeedRetryException) {
            ackMessage.setNext(((NeedRetryException) exception).getNext());
        }

        String registryURL = getRegistryURL(message);
        try {
            AcknowledgeAgent.acknowledge(registryURL, ackMessage);
        } catch (Throwable e) {
            scope.addAnnotation(Constants.QTRACE_STATUS, Constants.QTRACE_STATUS_ERROR);
            scope.addAnnotation(Constants.EXCEPTION_KEY, e.getMessage());
            LOGGER.error("[CRITICAL] Message ack error: \n	registry: {}\n	message: {}", registryURL, ackMessage, e);
        } finally {
            inProcessHolder.dequeue(message);
            if (exception != null) {
                scope.addAnnotation(Constants.QTRACE_STATUS, Constants.QTRACE_STATUS_ERROR);
                scope.addAnnotation(Constants.EXCEPTION_KEY, exception.toString());
            }
            scope.close();
        }
    }

    private int getActiveCount(Executor executor) {
        if (executor instanceof PooledExecutor) {
            return ((PooledExecutor) executor).getActiveCount();
        }

        if (executor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) executor).getActiveCount();
        }

        return 0;
    }

    private String getRegistryURL(BaseMessage message) {
        String registryURL = message.getStringProperty(BaseMessage.keys.qmq_registry);
        if (registryURL != null && registryURL.length() > 0) return registryURL;
        String registry = register.registry();
        if (registry != null) {
            String group = message.getStringProperty(BaseMessage.keys.qmq_brokerGroupName);
            return URLUtils.buildZKUrl(registry, brokerGroupRoot + "/" + group);
        }
        return StringUtils.EMPTY;
    }
}
