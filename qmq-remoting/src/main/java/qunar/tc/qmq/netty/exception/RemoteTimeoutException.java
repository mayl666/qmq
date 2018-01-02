package qunar.tc.qmq.netty.exception;

/**
 * @author zhenyu.nie created on 2017 2017/7/6 16:44
 */
public class RemoteTimeoutException extends RemoteException {

    private static final long serialVersionUID = -7427009787627990391L;

    public RemoteTimeoutException() {
    }

    public RemoteTimeoutException(Throwable cause) {
        super(cause);
    }

    public RemoteTimeoutException(String address, long timeoutMs) {
        this(address, timeoutMs, null);
    }

    public RemoteTimeoutException(String address, long timeoutMs, Throwable cause) {
        super("remote timeout on address [" + address + "] with timeout [" + timeoutMs + "]ms", cause);
    }
}
