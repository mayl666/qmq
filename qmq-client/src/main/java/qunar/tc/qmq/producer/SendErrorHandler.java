package qunar.tc.qmq.producer;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.service.exceptions.MessageException;

/**
 * @author zhenyu.nie created on 2017 2017/7/5 17:23
 */
public interface SendErrorHandler {

    void error(ProduceMessage pm, Exception e);

    void failed(ProduceMessage pm, Exception e);

    void block(ProduceMessage pm, MessageException ex);

    void finish(ProduceMessage pm, Exception e);
}
