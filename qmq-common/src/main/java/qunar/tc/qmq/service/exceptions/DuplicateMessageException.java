package qunar.tc.qmq.service.exceptions;

/**
 * User: zhaohuiyu
 * Date: 12/25/12
 * Time: 12:30 PM
 */
public class DuplicateMessageException extends MessageException {

    private static final long serialVersionUID = 8267606930373695631L;

    public DuplicateMessageException(String messageId) {
        super(messageId, "Duplicated message");
    }
}
