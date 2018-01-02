package qunar.tc.qmq.consumer.pull.exception;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class SendMessageBackException extends Exception {
    public SendMessageBackException() {
        super();
    }

    public SendMessageBackException(String s) {
        super(s);
    }

    public SendMessageBackException(String message, Throwable cause) {
        super(message, cause);
    }

    public SendMessageBackException(Throwable cause) {
        super(cause);
    }
}
