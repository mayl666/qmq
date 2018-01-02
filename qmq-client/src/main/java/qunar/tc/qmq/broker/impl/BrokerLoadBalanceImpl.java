package qunar.tc.qmq.broker.impl;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerLoadBalance;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BrokerLoadBalanceImpl implements BrokerLoadBalance {

    private static final Supplier<BrokerLoadBalance> SUPPLIER = Suppliers.memoize(new Supplier<BrokerLoadBalance>() {
        @Override
        public BrokerLoadBalance get() {
            return new BrokerLoadBalanceImpl();
        }
    });

    public static BrokerLoadBalance getInstance() {
        return SUPPLIER.get();
    }

    private BrokerLoadBalanceImpl() {
    }

    @Override
    public BrokerGroupInfo loadBalance(BrokerClusterInfo cluster, BrokerGroupInfo lastGroup) {
        List<BrokerGroupInfo> groups = cluster.getGroups();
        if (lastGroup == null || lastGroup.getGroupIndex() < 0 || lastGroup.getGroupIndex() >= groups.size()) {
            BrokerGroupInfo group = selectRandom(groups);
            if (!group.isAvailable()) {
                return loadBalance(cluster, group);
            } else {
                return group;
            }
        } else {
            int index = lastGroup.getGroupIndex();
            for (int count = groups.size(); count > 0; count--) {
                index = (index + 1) % groups.size();
                BrokerGroupInfo nextGroup = groups.get(index);
                if (nextGroup.isAvailable()) {
                    return nextGroup;
                }
            }
        }
        return lastGroup;
    }

    private BrokerGroupInfo selectRandom(List<BrokerGroupInfo> groups) {
        int random = ThreadLocalRandom.current().nextInt(groups.size());
        return groups.get(random);
    }
}
