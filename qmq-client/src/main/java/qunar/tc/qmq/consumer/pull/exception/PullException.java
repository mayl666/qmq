package qunar.tc.qmq.consumer.pull.exception;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class PullException extends Exception {
    public PullException() {
        super();
    }

    public PullException(String s) {
        super(s);
    }

    public PullException(String message, Throwable cause) {
        super(message, cause);
    }

    public PullException(Throwable cause) {
        super(cause);
    }
}
