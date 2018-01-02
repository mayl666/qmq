package qunar.tc.qmq.service.exceptions;

/**
 * User: zhaohuiyu
 * Date: 12/25/12
 * Time: 12:30 PM
 */
public class BlockMessageException extends MessageException {
    private static final long serialVersionUID = 1068741830127606624L;

    public BlockMessageException(String messageId) {
        super(messageId, "block message");
    }
}
