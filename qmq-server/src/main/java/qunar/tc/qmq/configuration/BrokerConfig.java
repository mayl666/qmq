package qunar.tc.qmq.configuration;

import com.google.common.base.Strings;
import qunar.tc.qmq.base.BrokerRole;
import qunar.tc.qmq.utils.NetworkUtils;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public final class BrokerConfig {
    private static final BrokerConfig CONFIG = new BrokerConfig();

    private final String brokerName;
    private final BrokerRole brokerRole;
    private final String brokerAddress;
    private volatile String masterAddress;

    private BrokerConfig() {
        brokerName = loadBrokerName();
        brokerRole = loadBrokerRole();
        brokerAddress = NetworkUtils.getLocalAddress();
        if (brokerRole == BrokerRole.SLAVE) {
            masterAddress = loadMasterAddress();
        }
    }

    public static String getBrokerName() {
        return CONFIG.brokerName;
    }

    public static BrokerRole getBrokerRole() {
        return CONFIG.brokerRole;
    }

    public static String getBrokerAddress() {
        return CONFIG.brokerAddress;
    }

    public static String getMasterAddress() {
        return CONFIG.masterAddress;
    }

    private String loadBrokerName() {
        final String name = System.getProperty("broker.name");
        if (Strings.isNullOrEmpty(name)) {
            throw new RuntimeException("环境变量 broker.name 未设置");
        }

        return name;
    }

    private BrokerRole loadBrokerRole() {
        final String role = System.getProperty("broker.role");
        if (Strings.isNullOrEmpty(role)) {
            throw new RuntimeException("环境变量 broker.role 未设置");
        }

        if (("master").equalsIgnoreCase(role)) {
            return BrokerRole.MASTER;
        } else if (("slave").equalsIgnoreCase(role)) {
            return BrokerRole.SLAVE;
        } else {
            throw new RuntimeException("未知的 broker role");
        }
    }

    private String loadMasterAddress() {
        final String masterAddress = System.getProperty("master.address");
        if (Strings.isNullOrEmpty(masterAddress)) {
            throw new RuntimeException("环境变量 master.address 未设置");
        }

        return masterAddress;
    }
}
