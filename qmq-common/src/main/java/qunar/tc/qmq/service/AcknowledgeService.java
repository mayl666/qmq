package qunar.tc.qmq.service;

import java.util.List;

import qunar.tc.qmq.base.ACKMessage;

/**
 * User: zhaohuiyu
 * Date: 12/27/12
 * Time: 6:42 PM
 */
public interface AcknowledgeService {
    void acknowledge(List<ACKMessage> messages);
}
