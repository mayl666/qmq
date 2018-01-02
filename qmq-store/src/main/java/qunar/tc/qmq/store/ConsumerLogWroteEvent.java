package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2017/7/13
 */
public class ConsumerLogWroteEvent {
    private final String subject;
    private final boolean success;

    public ConsumerLogWroteEvent(String subject, boolean success) {
        this.subject = subject;
        this.success = success;
    }

    public String getSubject() {
        return subject;
    }

    public boolean isSuccess() {
        return success;
    }
}
