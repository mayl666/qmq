package qunar.tc.qmq.consumer.pull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.metrics.Metrics;
import qunar.tc.qmq.NeedRetryException;
import qunar.tc.qmq.ReliabilityLevel;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.common.QmqLogger;
import qunar.tc.qmq.consumer.BaseMessageHandler;
import qunar.tc.qmq.consumer.pull.model.AckEntry;
import qunar.tc.qtracer.Constants;
import qunar.tc.qtracer.QTraceScope;
import qunar.tc.qtracer.QTracer;

/**
 * @author yiqun.fan create on 17-8-19.
 */
class AckHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(AckHelper.class);

    static void ack(PulledMessage message, Throwable throwable) {
        BaseMessageHandler.printError(message, throwable);
        final AckEntry ackEntry = message.getAckEntry();
        if (ReliabilityLevel.isLow(message)) {
            ackEntry.ack();
            return;
        }
        if (throwable == null) {
            ackEntry.ack();
            return;
        }
        final BaseMessage ackMsg = new BaseMessage(message);
        if (throwable instanceof NeedRetryException) {
            ackEntry.ackDelay(message.times() + 1, ((NeedRetryException) throwable).getNext(), ackMsg);
        } else {
            ackEntry.nack(message.times() + 1, ackMsg);
        }
    }

    static void ackWithQtrace(PulledMessage message, Throwable throwable) {
        BaseMessageHandler.printError(message, throwable);
        final AckEntry ackEntry = message.getAckEntry();
        if (ReliabilityLevel.isLow(message)) {
            QTracer.addTimelineAnnotation("LowReliabilityLevel");
            ackEntry.ack();
            return;
        }

        QTraceScope scope = null;
        try {
            scope = BaseMessageHandler.startAckTrace(message);
            if (throwable == null) {
                ackEntry.ack();
            } else {
                final BaseMessage ackMsg = new BaseMessage(message);
                if (throwable instanceof NeedRetryException) {
                    ackEntry.ackDelay(message.times() + 1, ((NeedRetryException) throwable).getNext(), ackMsg);
                } else {
                    ackEntry.nack(message.times() + 1, ackMsg);
                }
            }
        } catch (Exception e) {
            QmqLogger.log(message, "ack exception: " + e.getMessage());
            LOGGER.error("ack exception.", e);
            Metrics.counter("qmq_pull_ackError").tag("subject", message.getSubject()).delta().get().inc();
        } finally {
            if (scope != null) {
                if (throwable != null) {
                    scope.addAnnotation(Constants.QTRACE_STATUS, Constants.QTRACE_STATUS_ERROR);
                    scope.addAnnotation(Constants.EXCEPTION_KEY, throwable.toString());
                }
                scope.close();
            }
        }
    }
}
