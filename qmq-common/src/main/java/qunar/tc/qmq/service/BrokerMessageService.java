package qunar.tc.qmq.service;

import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.List;
import java.util.Map;

/**
 * User: zhaohuiyu
 * Date: 12/24/12
 * Time: 3:31 PM
 */
public interface BrokerMessageService {
    
    Map<String, MessageException> send(List<BaseMessage> messages);

    boolean isReceived(String messageId);
}
