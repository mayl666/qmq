package qunar.tc.qmq.meta;

import qunar.tc.qmq.base.BrokerState;
import qunar.tc.qmq.meta.store.DatabaseStore;
import qunar.tc.qmq.meta.store.Store;

/**
 * @author yunfeng.yang
 * @since 2017/9/4
 */
public class StoreTest {
    public static void main(String[] args) {
        Store store = new DatabaseStore();
        store.insertOrUpdateBrokerGroup("def", "123456", BrokerState.R);
        store.insertOrUpdateBrokerGroup("def", "543210", BrokerState.R);
    }
}
