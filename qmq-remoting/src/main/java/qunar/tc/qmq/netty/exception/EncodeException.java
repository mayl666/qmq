package qunar.tc.qmq.netty.exception;

/**
 * @author yiqun.fan create on 17-7-6.
 */
public class EncodeException extends Exception {

    public EncodeException(String s) {
        super(s);
    }

    public EncodeException(String s, Throwable cause) {
        super(s, cause);
    }
}
