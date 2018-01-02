package qunar.tc.qmq.netty.exception;

import qunar.tc.qmq.service.exceptions.MessageException;

/**
 * @author yiqun.fan create on 17-9-8.
 */
public class BrokerRejectException extends MessageException {

    public BrokerRejectException(String messageId) {
        super(messageId, "reject");
    }
}
