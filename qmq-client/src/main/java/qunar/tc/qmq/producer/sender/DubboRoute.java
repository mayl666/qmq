package qunar.tc.qmq.producer.sender;

import com.alibaba.dubbo.common.URL;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import qunar.tc.qmq.utils.Constants;
import qunar.tc.qmq.utils.URLUtils;

import java.util.concurrent.ConcurrentMap;

/**
 * @author zhenyu.nie created on 2017 2017/7/3 12:11
 */
class DubboRoute implements Route {

    private static final ConcurrentMap<Route, Connection> connections = Maps.newConcurrentMap();

    private final String zkAddress;

    private final String group;

    private final String registryUrl;

    DubboRoute(String zkAddress, String group) {
        this.zkAddress = zkAddress;
        this.group = group;
        this.registryUrl = getRegistryURL();
    }

    @Override
    public boolean isNewRoute() {
        return !connections.containsKey(this);
    }

    @Override
    public void preHeat() {
        Connection connection = connections.get(this);
        if (connection != null) {
            connection.preHeat();
            return;
        }

        DubboConnection newConnection = new DubboConnection(registryUrl);
        newConnection.preHeat();
        if (connections.putIfAbsent(this, newConnection) != null) {
            newConnection.destroy();
        }
    }

    @Override
    public Connection route() {
        Connection connection = connections.get(this);
        if (connection != null) {
            return connection;
        }

        Connection newConnection = new DubboConnection(registryUrl);
        Connection oldConnection = connections.putIfAbsent(this, newConnection);
        if (oldConnection == null) {
            return newConnection;
        } else {
            newConnection.destroy();
            return oldConnection;
        }
    }

    private String getRegistryURL() {
        String registry = computeRegistry(StringUtils.trim(Preconditions.checkNotNull(zkAddress)));
        return URLUtils.buildZKUrl(registry, String.format("%s/%s", Constants.BROKER_GROUP_ROOT, group));
    }

    private String computeRegistry(String registryURL) {
        if (registryURL.startsWith("zookeeper")) {
            URL url = URL.valueOf(registryURL);
            return url.getBackupAddress();
        }
        return registryURL;
    }

    public static void destroy() {
        for (Connection connection : connections.values()) {
            connection.destroy();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DubboRoute route = (DubboRoute) o;

        if (group != null ? !group.equals(route.group) : route.group != null) return false;
        if (zkAddress != null ? !zkAddress.equals(route.zkAddress) : route.zkAddress != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = zkAddress != null ? zkAddress.hashCode() : 0;
        result = 31 * result + (group != null ? group.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DubboRoute{" +
                "zkAddress='" + zkAddress + '\'' +
                ", group='" + group + '\'' +
                '}';
    }
}
