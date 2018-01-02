package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.consumer.ConsumeMessage;
import qunar.tc.qmq.consumer.pull.model.AckEntry;

/**
 * @author yiqun.fan create on 17-7-20.
 */
public class PulledMessage extends ConsumeMessage {
    private final AckEntry ackEntry;
    private final AckHook ackHook;

    PulledMessage(BaseMessage message, AckEntry ackEntry, AckHook ackHook) {
        super(message);
        this.ackEntry = ackEntry;
        this.ackHook = ackHook;
    }

    AckEntry getAckEntry() {
        return ackEntry;
    }

    @Override
    public void ack(long elapsed, Throwable e) {
        if (ackHook != null) {
            ackHook.call(this, e);
        } else {
            AckHelper.ack(this, e);
        }
    }
}
