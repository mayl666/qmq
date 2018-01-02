package qunar.tc.qmq.broker;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public interface BrokerLoadBalance {
    BrokerGroupInfo loadBalance(BrokerClusterInfo cluster, BrokerGroupInfo lastGroup);
}
