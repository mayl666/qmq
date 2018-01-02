package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.Message;

/**
 * User: yee.wang
 * Date: 9/15/14
 * Time: 5:39 PM
 */
public class LocalConfigRouter implements Router {

    private final DubboRoute route;

    public LocalConfigRouter(String zkAddress, String group) {
        this.route = new DubboRoute(zkAddress, group);
    }

    @Override
    public Connection route(Message message) {
        return route.route();
    }
}
