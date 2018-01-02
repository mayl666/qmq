package qunar.tc.qmq.store;

/**
 * @author yunfeng.yang
 * @since 2017/7/11
 */
public class MessageLogWroteEvent {
    private final String subject;
    private final AppendMessageResult<MessageSequence> result;

    public MessageLogWroteEvent(String subject, AppendMessageResult<MessageSequence> result) {
        this.subject = subject;
        this.result = result;
    }

    public String getSubject() {
        return subject;
    }

    public AppendMessageResult<MessageSequence> getResult() {
        return result;
    }
}