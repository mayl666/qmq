package qunar.tc.qmq.service.exceptions;

/**
 * User: zhaohuiyu
 * Date: 1/17/13
 * Time: 5:21 PM
 */
public class ExpiredException extends MessageException {
    private static final long serialVersionUID = 6264083636310538269L;

    public ExpiredException(String messageId) {
        super(messageId, "Message expired");
    }
}
