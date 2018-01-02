package qunar.tc.qmq.model;

import com.google.common.util.concurrent.SettableFuture;
import qunar.tc.qmq.base.RawMessage;

/**
 * @author yunfeng.yang
 * @since 2017/8/8
 */
public class ReceivingMessage {
    private final RawMessage message;
    private final SettableFuture<ReceiveResult> promise;

    private final long receivedTime;

    public ReceivingMessage(RawMessage message, long receivedTime) {
        this.message = message;
        this.receivedTime = receivedTime;
        this.promise = SettableFuture.create();
    }

    public RawMessage getMessage() {
        return message;
    }

    public SettableFuture<ReceiveResult> promise() {
        return promise;
    }

    public long getReceivedTime() {
        return receivedTime;
    }

    public void release() {
        message.release();
    }

    public String getMessageId() {
        return message.getHeader().getMessageId();
    }

    public void done(ReceiveResult result) {
        promise.set(result);
    }

    public String getSubject() {
        return message.getHeader().getSubject();
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > message.getHeader().getExpireTime();
    }

    public boolean isHigh() {
        return message.isHigh();
    }

}
