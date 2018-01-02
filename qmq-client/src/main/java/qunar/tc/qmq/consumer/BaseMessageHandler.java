package qunar.tc.qmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.*;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.consumer.handler.IdempotentCheckerFilter;
import qunar.tc.qmq.consumer.handler.QTraceFilter;
import qunar.tc.qmq.serializer.Serializer;
import qunar.tc.qmq.serializer.SerializerFactory;
import qunar.tc.qtracer.Constants;
import qunar.tc.qtracer.QTraceClient;
import qunar.tc.qtracer.QTraceScope;
import qunar.tc.qtracer.QTracer;
import qunar.tc.qtracer.impl.QTraceClientGetter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BaseMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseMessageHandler.class);

    protected final Executor executor;
    protected final MessageListener listener;
    private final List<Filter> filters;
    protected final Filter qtraceFilter;
    protected final Serializer serializer;

    public BaseMessageHandler(Executor executor, MessageListener listener) {
        this.executor = executor;
        this.listener = listener;
        this.filters = new ArrayList<>();
        buildFilterChain(listener);
        this.qtraceFilter = new QTraceFilter();
        this.serializer = SerializerFactory.create();
    }

    private void buildFilterChain(MessageListener listener) {
        if (listener instanceof FilterAttachable) {
            this.filters.addAll(FilterAttachable.class.cast(listener).filters());
        }
        if (listener instanceof IdempotentAttachable) {
            this.filters.add(new IdempotentCheckerFilter(IdempotentAttachable.class.cast(listener).getIdempotentChecker()));
        }
    }

    protected boolean triggerBeforeOnMessage(ConsumeMessage message, Map<String, Object> filterContext) {
        for (int i = 0; i < filters.size(); ++i) {
            message.setProcessedFilterIndex(i);
            if (!filters.get(i).preOnMessage(message, filterContext)) {
                return false;
            }
        }
        return true;
    }

    protected void applyPostOnMessage(ConsumeMessage message, Throwable ex, Map<String, Object> filterContext) {
        int processedFilterIndex = message.getProcessedFilterIndex();
        for (int i = processedFilterIndex; i >= 0; --i) {
            try {
                filters.get(i).postOnMessage(message, ex, filterContext);
            } catch (Throwable e) {
                LOGGER.error("post filter failed", e);
            }
        }
    }

    public static QTraceScope startAckTrace(BaseMessage message) {
        QTraceClient qtracer = QTraceClientGetter.getClient();
        if (message.getBooleanProperty(QTraceFilter.QSCHEDULE_DESC)) {
            QTraceScope result = qtracer.startTrace(QTraceFilter.QSCHEDULE_DESC + ".ack");
            result.addAnnotation(Constants.QTRACE_TYPE, QTraceFilter.QSCHEDULE_QTRACE_TYPE);
            result.addAnnotation("jobName", message.getSubject());
            result.addAnnotation("taskId", message.getMessageId());
            return result;
        } else {
            QTraceScope result = qtracer.startTrace("qmq.ack");
            result.addAnnotation(Constants.QTRACE_TYPE, Constants.QTRACE_TYPE_QMQ);
            result.addAnnotation("messageId", message.getMessageId());
            final String prefix = message.getStringProperty(BaseMessage.keys.qmq_prefix);
            final String consumerGroup = message.getStringProperty(BaseMessage.keys.qmq_consumerGroupName);
            result.addAnnotation("prefix", prefix);
            result.addAnnotation("group", consumerGroup);
            return result;
        }
    }

    public static void printError(BaseMessage message, Throwable e) {
        if (e == null) return;
        if (e instanceof NeedRetryException) return;
        LOGGER.error("message process error. subject={}, msgId={}, times={}",
                message.getSubject(), message.getMessageId(), message.times(), e);
    }

    public abstract static class HandleTask implements Runnable {
        protected final ConsumeMessage message;
        private final BaseMessageHandler handler;
        private volatile int localRetries = 0;  // 本地重试次数

        protected HandleTask(ConsumeMessage message, BaseMessageHandler handler) {
            this.message = message;
            this.handler = handler;
        }

        @Override
        public void run() {
            final long start = System.currentTimeMillis();
            final Map<String, Object> filterContext = new HashMap<>();
            message.localRetries(localRetries);
            message.setSerializer(handler.serializer);
            message.setFilterContext(filterContext);

            Throwable exception = null;
            boolean reQueued = false;
            try {
                handler.qtraceFilter.preOnMessage(message, filterContext);
                if (!handler.triggerBeforeOnMessage(message, filterContext)) return;
                handler.listener.onMessage(message);
            } catch (NeedRetryException e) {
                exception = e;
                try {
                    reQueued = localRetry(e);
                } catch (Throwable ex) {
                    exception = ex;
                }
            } catch (Throwable e) {
                exception = e;
            } finally {
                triggerAfterCompletion(reQueued, start, exception, filterContext);
                handler.qtraceFilter.postOnMessage(message, exception, filterContext);
            }
        }

        private boolean localRetry(NeedRetryException e) {
            boolean reQueued = false;
            if (isRetryImmediately(e)) {
                QTracer.addTimelineAnnotation("local retry");
                try {
                    ++localRetries;
                    handler.executor.execute(this);
                    reQueued = true;
                } catch (RejectedExecutionException re) {
                    message.localRetries(localRetries);
                    try {
                        handler.listener.onMessage(message);
                    } catch (NeedRetryException ne) {
                        localRetry(ne);
                    }
                }
            }
            return reQueued;
        }

        private boolean isRetryImmediately(NeedRetryException e) {
            long next = e.getNext();
            return next - System.currentTimeMillis() <= 10;
        }

        private void triggerAfterCompletion(boolean reQueued, long start, Throwable exception, Map<String, Object> filterContext) {
            if (message.isAutoAck() || exception != null) {
                handler.applyPostOnMessage(message, exception, filterContext);
                if (!reQueued) {
                    doAck(System.currentTimeMillis() - start, exception);
                }
            }
        }

        protected abstract void doAck(long elapsed, Throwable exception);
    }
}
