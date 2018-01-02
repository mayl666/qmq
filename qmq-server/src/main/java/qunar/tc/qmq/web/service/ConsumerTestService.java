package qunar.tc.qmq.web.service;

import qunar.tc.qmq.MessageListener;

/**
 * @author yunfeng.yang
 * @since 2017/7/12
 */
public interface ConsumerTestService {
    void addListener(String subjectPrefix, String consumerGroup, MessageListener messageListener);

    void stopListener(String subjectPrefix, String consumerGroup);
}
