package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.Message;

/**
 * User: yee.wang
 * Date: 9/15/14
 * Time: 5:30 PM
 */
public interface Router {

    Route NOROUTE = new NopRoute();

    Connection route(Message message);
}
