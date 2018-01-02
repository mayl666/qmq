package qunar.tc.qmq.producer.sender;

import com.google.common.base.Preconditions;
import qunar.tc.qmq.Message;

/**
 * Created by zhaohui.yu
 * 9/13/17
 */
abstract class AbstractRouterManager implements RouterManager {

    private Router router;

    @Override
    public String registryOf(Message message) {
        return router.route(message).url();
    }

    void setRouter(Router router) {
        this.router = router;
    }

    @Override
    public Connection routeOf(Message message) {
        Connection connection = router.route(message);
        Preconditions.checkState(connection != NopRoute.NOP_CONNECTION, "与broker连接失败，可能是配置错误，请联系TCDev");
        return connection;
    }
}
