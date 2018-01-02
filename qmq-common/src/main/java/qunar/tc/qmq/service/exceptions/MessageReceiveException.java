package qunar.tc.qmq.service.exceptions;

/**
 * User: zhaohuiyu
 * Date: 12/25/12
 * Time: 11:50 AM
 */
public class MessageReceiveException extends MessageException {

    private static final long serialVersionUID = 4578046555069594620L;

    public MessageReceiveException(String messageId, Throwable throwable) {
        super(messageId, (throwable == null) ? null : throwable.getMessage(), throwable);
    }
}
