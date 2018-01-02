package qunar.tc.qmq.netty.exception;

/**
 * @author zhenyu.nie created on 2017 2017/7/6 16:44
 */
public class RemoteException extends Exception {

    private static final long serialVersionUID = -7144986221917657039L;

    public RemoteException() {
    }

    public RemoteException(String message) {
        super(message);
    }

    public RemoteException(String message, Throwable cause) {
        super(message, cause);
    }

    public RemoteException(Throwable cause) {
        super(cause);
    }
}
