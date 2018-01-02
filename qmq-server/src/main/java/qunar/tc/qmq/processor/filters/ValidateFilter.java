package qunar.tc.qmq.processor.filters;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import qunar.tc.qmq.model.ReceivingMessage;
import qunar.tc.qmq.processor.Invoker;

/**
 * User: zhaohuiyu Date: 4/1/13 Time: 7:01 PM
 */
class ValidateFilter implements ReceiveFilter {

    private static final int MAX_MESSAGE_LENGTH = 64 * 1024;

    @Override
    public void invoke(Invoker invoker, ReceivingMessage message) {
        Preconditions.checkNotNull(message, "message not null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(message.getMessageId()), "message id should not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(message.getSubject()), "message subject should not be empty");
        // TODO 一个消息抛异常，所有消息都发不出去
        // 只有可靠消息才检查这个
//        if (message.isHigh()) {
//            Preconditions.checkArgument(message.getMessage().getMessageSize() < MAX_MESSAGE_LENGTH,
//                    "message should less than 64k, subject: " + message.getSubject() + ", messageId: " + message.getMessageId());
//        }
        invoker.invoke(message);
    }
}