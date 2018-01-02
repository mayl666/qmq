package qunar.tc.qmq.consumer.handler;

import com.google.common.collect.ImmutableMap;
import qunar.tc.qmq.Filter;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qtracer.*;
import qunar.tc.qtracer.impl.QTraceClientGetter;
import qunar.tc.qtracer.util.TraceContextSerde;

import java.util.Map;

/**
 * Created by zhaohui.yu
 * 15/12/9
 */
public class QTraceFilter implements Filter {
    private static final String ATTATCHMENT_KEY = "scope";

    public static final String QSCHEDULE_DESC = "qschedule";
    public static final String QSCHEDULE_QTRACE_TYPE = "QSCHEDULE";

    private final QTraceClient client = QTraceClientGetter.getClient();

    @Override
    public boolean preOnMessage(Message message, Map<String, Object> filterContext) {
        QTraceScope scope = createQTraceScope(message, "process");
        filterContext.put(ATTATCHMENT_KEY, scope);
        return true;
    }

    @Override
    public void postOnMessage(Message message, Throwable e, Map<String, Object> filterContext) {
        Object temp = filterContext.get(ATTATCHMENT_KEY);
        if (temp == null) return;

        if (e != null) {
            QTracer.addKVAnnotation(Constants.QTRACE_STATUS, Constants.QTRACE_STATUS_ERROR);
            QTracer.addKVAnnotation(Constants.EXCEPTION_KEY, e.getMessage());
        }
        ((QTraceScope) temp).close();
    }

    private QTraceScope createQTraceScope(Message message, String desc) {
        if (message.getBooleanProperty(QSCHEDULE_DESC)) {
            QTraceScope result = this.client.startTrace(QSCHEDULE_DESC + "." + desc);
            result.addAnnotation(Constants.QTRACE_TYPE, QSCHEDULE_QTRACE_TYPE);
            result.addAnnotation("jobName", message.getSubject());
            result.addAnnotation("taskId", message.getMessageId());
            return result;
        } else {
            int i = message.localRetries() + 1;
            String spanId = message.getStringProperty(BaseMessage.keys.qmq_spanId.name());
            QTraceScope result = null;
            if (spanId == null || spanId.length() == 0 || Constants.DO_NOT_TRACE_SPANID.equals(spanId)) {
                result = this.client.startTrace("qmq." + desc);
            } else {
                spanId = spanId + "." + i;
                String traceId = message.getStringProperty(BaseMessage.keys.qmq_traceId.name());
                ImmutableMap<String, String> traceContext = TraceContextSerde.fromJson(message.getStringProperty(BaseMessage.keys.qmq_traceContext.name()));
                result = this.client.startTrace("qmq." + desc, new TraceInfo(traceId, spanId, traceContext));
            }
            result.addAnnotation("subject", message.getSubject());
            result.addAnnotation("messageId", message.getMessageId());
            result.addAnnotation("reliability", message.getReliabilityLevel().name());
            result.addAnnotation(Constants.QTRACE_TYPE, Constants.QTRACE_TYPE_QMQ);
            return result;
        }
    }
}
