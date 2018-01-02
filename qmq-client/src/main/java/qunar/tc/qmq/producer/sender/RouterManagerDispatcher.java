package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.Message;

/**
 * @author zhenyu.nie created on 2017 2017/7/3 14:18
 */
public interface RouterManagerDispatcher {

    void init();

    RouterManager dispatch(Message message);

    void destroy();
}
