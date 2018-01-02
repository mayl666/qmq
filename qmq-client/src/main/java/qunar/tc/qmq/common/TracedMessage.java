package qunar.tc.qmq.common;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import qunar.tc.qmq.ReliabilityLevel;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.producer.QTracerConfig;
import qunar.tc.qtracer.*;
import qunar.tc.qtracer.impl.QTraceClientGetter;
import qunar.tc.qtracer.util.TraceContextSerde;

/**
 * User: zhaohuiyu
 * Date: 12/29/14
 * Time: 2:49 PM
 */
public class TracedMessage extends BaseMessage implements QTraceScope {
    private static final long serialVersionUID = -1541226325035453504L;

    public static final String QSCHEDULE_DESC = "qschedule";

    private static final String QSCHEDULE_QTRACE_TYPE = "QSCHEDULE";

    private final QTraceScope scope;

    private final BaseMessage message;

    private TracedMessage(BaseMessage message, String desc) {
        super(message);
        this.message = message;

        this.scope = startTrace(message, desc);
    }

    private QTraceScope startTrace(BaseMessage message, String desc) {
        String traceId = getStringProperty(keys.qmq_traceId);
        String spanId = getStringProperty(keys.qmq_spanId);
        ImmutableMap<String, String> traceContext = TraceContextSerde.fromJson(getStringProperty(keys.qmq_traceContext));

        //如果消息的可靠级别是低，那么trace的逻辑是:
        //如果当前已经开启了trace，则trace，否则不开启新trace，也不参照采样器
        if (ReliabilityLevel.isLow(this) && Strings.isNullOrEmpty(traceId)) {
            traceId = Constants.DO_NOT_TRACE_TRACEID;
        }
        QTraceClient client = QTraceClientGetter.getClient();
        QTraceScope result = createQTraceScope(message, desc, traceId, spanId, traceContext, client);
        result.addAnnotation("subject", message.getSubject());
        result.addAnnotation("messageId", message.getMessageId());
        result.addAnnotation("reliability", message.getReliabilityLevel().name());
        return result;
    }

    private QTraceScope createQTraceScope(BaseMessage message, String desc, String traceId, String spanId, ImmutableMap<String, String> traceContext, QTraceClient client) {
        if (message.getBooleanProperty(QSCHEDULE_DESC)) {
            QTraceScope result = client.startTrace(QSCHEDULE_DESC + "." + desc, new TraceInfo(traceId, spanId, traceContext));
            result.addAnnotation(Constants.QTRACE_TYPE, QSCHEDULE_QTRACE_TYPE);
            return result;
        } else {
            QTraceScope result = client.startTrace("qmq." + desc, new TraceInfo(traceId, spanId, traceContext));
            result.addAnnotation(Constants.QTRACE_TYPE, Constants.QTRACE_TYPE_QMQ);
            return result;
        }
    }

    public static TracedMessage producer(BaseMessage message, QTracerConfig qTracerConfig) {
        TracedMessage traced = new TracedMessage(message, "producer");
        QTraceClient client = QTraceClientGetter.getClient();
        String currentTraceId = client.getCurrentTraceId();
        String nextChildSpanId = client.getNextChildSpanId();
        ImmutableMap<String, String> traceContext = client.getTraceContext();

        if (!Strings.isNullOrEmpty(currentTraceId) && !Strings.isNullOrEmpty(nextChildSpanId)) {
            traced.message.setProperty(keys.qmq_traceId, currentTraceId);
            traced.message.setProperty(keys.qmq_spanId, nextChildSpanId);
            if (traceContext != null && !traceContext.isEmpty()) {
                traced.message.setProperty(keys.qmq_traceContext, TraceContextSerde.toJson(traceContext));
            }
        }

        qTracerConfig.configure(message);

        traced.detach();
        return traced;
    }

    @Override
    public void addAnnotation(String key, String value) {
        scope.addAnnotation(key, value);
    }

    @Override
    public void addTimeAnnotation(String msg) {
        scope.addTimeAnnotation(msg);
    }

    @Override
    public void addTraceContext(String key, String value) {
        scope.addTraceContext(key, value);
    }

    @Override
    public ImmutableMap<String, String> getTraceContext() {
        return scope.getTraceContext();
    }

    @Override
    public void close() {
        scope.close();
    }

    @Override
    public Span detach() {
        return scope.detach();
    }

    public BaseMessage message() {
        return message;
    }
}
