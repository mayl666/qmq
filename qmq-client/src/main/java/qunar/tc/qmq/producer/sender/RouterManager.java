package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.Message;
import qunar.tc.qmq.producer.QueueSender;

/**
 * Created by yee.wang on 2014/9/17.
 */
public interface RouterManager {

    void init();

    String name();

    String registryOf(Message message);

    Connection routeOf(Message message);

    QueueSender getSender();

    void destroy();
}
