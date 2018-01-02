package qunar.tc.qmq.netty.client;

import com.google.common.base.Objects;
import qunar.tc.qmq.protocol.Datagram;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResponseFuture {
    private final int opaque;
    private final long timeout;
    private final Callback callback;
    private final long beginTime = System.currentTimeMillis();
    private volatile long sendedTime = -1;
    private volatile long receiveTime = -1;

    private volatile boolean sendOk = false;
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile Datagram response;
    private volatile Throwable cause;
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);

    ResponseFuture(int opaque, long timeoutMs, Callback callback) {
        this.opaque = opaque;
        this.timeout = timeoutMs;
        this.callback = callback;
    }

    public int getOpaque() {
        return opaque;
    }

    public long getTimeout() {
        return timeout;
    }

    public long getBeginTime() {
        return beginTime;
    }

    public boolean isSendOk() {
        return sendOk;
    }

    void setSendOk(boolean sendOk) {
        this.sendedTime = System.currentTimeMillis();
        this.sendOk = sendOk;
    }

    public long getReceiveTime() {
        return receiveTime;
    }

    public boolean isTimeout() {
        if (timeout < 0) {
            return false;
        }
        long endTimestamp = receiveTime > 0 ? receiveTime : System.currentTimeMillis();
        return (endTimestamp - this.beginTime) > this.timeout;
    }

    public Datagram getResponse() {
        return response;
    }

    public void setResponse(final Datagram response) {
        if (response != null && !sendOk) {
            setSendOk(true);
        }
        this.receiveTime = System.currentTimeMillis();
        this.response = response;
        this.latch.countDown();
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    Datagram waitResponse(final long timeoutMs) throws InterruptedException {
        this.latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        return this.response;
    }

    void executeCallbackOnlyOnce() {
        if (callback != null) {
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                callback.processResponse(this);
            }
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("opaque", opaque)
                .add("begin", beginTime)
                .add("send", sendedTime)
                .add("received", receiveTime)
                .add("sendOk", sendOk)
                .add("cause", cause == null ? "null" : cause.getMessage())
                .toString();
    }

    public interface Callback {
        void processResponse(ResponseFuture responseFuture);
    }
}
