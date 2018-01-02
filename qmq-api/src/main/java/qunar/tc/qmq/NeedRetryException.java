package qunar.tc.qmq;

/**
 * Created by zhaohui.yu
 * 15/12/2
 * <p/>
 * qmq会根据该异常里的时间进行重试间隔控制
 */
public class NeedRetryException extends RuntimeException {
    private final long next;

    public NeedRetryException(long next, String message) {
        super(message);
        this.next = next;
    }

    /**
     * WARNING WARNING
     * 使用该构造函数构造的异常会立即重试
     *
     * @param message
     */
    public NeedRetryException(String message) {
        this(System.currentTimeMillis(), message);
    }

    public long getNext() {
        return next;
    }
}
