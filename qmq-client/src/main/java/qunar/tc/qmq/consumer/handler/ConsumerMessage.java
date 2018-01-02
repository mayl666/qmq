package qunar.tc.qmq.consumer.handler;

import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.consumer.ConsumeMessage;

import java.util.Map;

class ConsumerMessage extends ConsumeMessage {
    private static final long serialVersionUID = -5759999937315214074L;

    private transient final MessageHandler handler;

    ConsumerMessage(BaseMessage message, MessageHandler handler) {
        super(message);
        this.handler = handler;
    }

    @Override
    public void ack(long elapsed, Throwable e) {
        handler.sendAck(this, elapsed, e);
    }
}
