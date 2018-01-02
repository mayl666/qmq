package qunar.tc.qmq.broker;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BrokerGroupInfo {
    private final int groupIndex;
    private final String groupName;
    private final String master;
    private final List<String> slaves;
    private final AtomicBoolean available = new AtomicBoolean(true);

    public BrokerGroupInfo(int groupIndex, String groupName, String master, List<String> slaves) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(groupName), "groupName不能是空");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(master), "master不能是空");
        this.groupIndex = groupIndex;
        this.groupName = groupName;
        this.master = master;
        this.slaves = slaves;
    }

    public int getGroupIndex() {
        return groupIndex;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getMaster() {
        return master;
    }

    public List<String> getSlaves() {
        return slaves;
    }

    public void setAvailable(boolean available) {
        this.available.set(available);
    }

    public boolean isAvailable() {
        return available.get();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("groupName", groupName)
                .add("master", master)
                .add("slaves", slaves)
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || (obj != null && obj instanceof BrokerGroupInfo && groupName.equals(((BrokerGroupInfo) obj).groupName));
    }

    @Override
    public int hashCode() {
        return groupName.hashCode();
    }
}
