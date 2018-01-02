package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.Message;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.ClientType;

/**
 * @author zhenyu.nie created on 2017 2017/7/5 16:29
 */
class NettyRouter implements Router {

    private final NettyProducerClient producerClient;
    private final BrokerService brokerService = BrokerServiceImpl.getInstance();

    NettyRouter(NettyProducerClient producerClient) {
        this.producerClient = producerClient;
    }

    @Override
    public Connection route(Message message) {
        BrokerClusterInfo brokerCluster = brokerService.getClusterBySubject(ClientType.PRODUCER, message.getSubject());
        return new NettyConnection(message.getSubject(), producerClient, brokerCluster);
    }
}
