package qunar.tc.qmq.consumer.pull;

import qunar.metrics.Gauge;
import qunar.metrics.Metrics;
import qunar.metrics.Timer;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.ReliabilityLevel;
import qunar.tc.qmq.common.QmqLogger;
import qunar.tc.qmq.consumer.BaseMessageHandler;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class PushConsumer extends BaseMessageHandler implements AckHook {
    private final String subject;
    private final String group;
    private final LinkedBlockingQueue<PulledMessage> messageBuffer;
    private final Timer sendToPushTimer;
    private final Timer sendToHandleTimer;

    PushConsumer(String subject, String group, MessageListener listener, Executor executor) {
        super(executor, listener);
        this.subject = subject;
        this.group = group;
        this.messageBuffer = new LinkedBlockingQueue<>();

        Metrics.gauge("qmq_pull_buffer_size").tag("subject", subject).tag("group", group).call(new Gauge() {
            @Override
            public double getValue() {
                return messageBuffer.size();
            }
        });
        this.sendToPushTimer = Metrics.timer("qmq_pull_createToPushTimer").tag("subject", subject).tag("group", group).get();
        this.sendToHandleTimer = Metrics.timer("qmq_pull_createToHandleTimer").tag("subject", subject).tag("group", group).get();
    }

    public String getSubject() {
        return subject;
    }

    public String getGroup() {
        return group;
    }

    boolean cleanLocalBuffer() {
        while (!messageBuffer.isEmpty()) {
            if (!push(messageBuffer.peek())) {
                return false;
            } else {
                messageBuffer.poll();
            }
        }
        return true;
    }

    void push(List<PulledMessage> messages) {
        for (int i = 0; i < messages.size(); i++) {
            final PulledMessage message = messages.get(i);
            if (!push(message)) {
                messageBuffer.addAll(messages.subList(i, messages.size()));
                break;
            }
        }
    }

    private boolean push(PulledMessage message) {
        sendToPushTimer.update(System.currentTimeMillis() - message.getCreatedTime().getTime(), TimeUnit.MILLISECONDS);
        QmqLogger.log(message, "收到一条消息,此消息会被匹配到: " + subject + "/" + group);
        if (ReliabilityLevel.isLow(message)) {
            AckHelper.ackWithQtrace(message, null);
        }
        HandleTaskImpl task = new HandleTaskImpl(message, this);
        try {
            executor.execute(task);
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    private final class HandleTaskImpl extends HandleTask {
        private final PulledMessage message;

        HandleTaskImpl(PulledMessage message, BaseMessageHandler handler) {
            super(message, handler);
            this.message = message;
        }

        @Override
        public void run() {
            sendToHandleTimer.update(System.currentTimeMillis() - message.getCreatedTime().getTime(), TimeUnit.MILLISECONDS);
            super.run();
        }

        @Override
        protected void doAck(long elapsed, Throwable exception) {
            AckHelper.ackWithQtrace(message, exception);
        }
    }

    @Override
    public void call(PulledMessage message, Throwable throwable) {
        applyPostOnMessage(message, throwable, new HashMap<>(message.getFilterContext()));
        AckHelper.ackWithQtrace(message, throwable);
    }
}
