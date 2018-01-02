package qunar.tc.qmq.broker;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BrokerClusterInfo {
    private final List<BrokerGroupInfo> groupList;
    private final Map<String, BrokerGroupInfo> groupMap;

    public BrokerClusterInfo() {
        this.groupList = Collections.emptyList();
        this.groupMap = Collections.emptyMap();
    }

    public BrokerClusterInfo(List<BrokerGroupInfo> groupList) {
        this.groupList = groupList;
        this.groupMap = Maps.newHashMapWithExpectedSize(groupList.size());
        for (BrokerGroupInfo group : groupList) {
            groupMap.put(group.getGroupName(), group);
        }
    }

    public List<BrokerGroupInfo> getGroups() {
        return groupList;
    }

    public BrokerGroupInfo getGroupByName(String groupName) {
        return groupMap.get(groupName);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("groups", groupList)
                .toString();
    }
}
