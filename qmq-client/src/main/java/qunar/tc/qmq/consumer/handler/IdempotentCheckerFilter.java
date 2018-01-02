package qunar.tc.qmq.consumer.handler;

import qunar.tc.qmq.Filter;
import qunar.tc.qmq.IdempotentChecker;
import qunar.tc.qmq.Message;
import qunar.tc.qtracer.QTracer;

import java.util.Map;

/**
 * Created by zhaohui.yu
 * 15/12/8
 */
public class IdempotentCheckerFilter implements Filter {

    private final IdempotentChecker idempotentChecker;

    public IdempotentCheckerFilter(IdempotentChecker idempotentChecker) {
        this.idempotentChecker = idempotentChecker;
    }

    @Override
    public boolean preOnMessage(Message message, Map<String, Object> filterContext) {
        if (idempotentChecker == null) return true;
        QTracer.addTimelineAnnotation("start idempotent");
        boolean processed = idempotentChecker.isProcessed(message);
        QTracer.addTimelineAnnotation("end idempotent");
        if (processed) {
            QTracer.addKVAnnotation("idempotent", "processed");
        }
        return !processed;
    }

    @Override
    public void postOnMessage(Message message, Throwable e, Map<String, Object> filterContext) {
        if (idempotentChecker == null) return;
        idempotentChecker.markProcessed(message, e);
    }
}
