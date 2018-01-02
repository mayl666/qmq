package qunar.tc.qmq.consumer.pull.exception;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class AckException extends Exception {
    public AckException() {
        super();
    }

    public AckException(String s) {
        super(s);
    }

    public AckException(String message, Throwable cause) {
        super(message, cause);
    }

    public AckException(Throwable cause) {
        super(cause);
    }
}
