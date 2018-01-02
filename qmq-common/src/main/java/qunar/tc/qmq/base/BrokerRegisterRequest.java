package qunar.tc.qmq.base;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public class BrokerRegisterRequest {
    private String groupName;
    private int brokerRole;
    private int brokerState;
    private int requestType;
    private String brokerAddress;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public int getRequestType() {
        return requestType;
    }

    public void setRequestType(int requestType) {
        this.requestType = requestType;
    }

    public int getBrokerRole() {
        return brokerRole;
    }

    public void setBrokerRole(int brokerRole) {
        this.brokerRole = brokerRole;
    }

    public int getBrokerState() {
        return brokerState;
    }

    public void setBrokerState(int brokerState) {
        this.brokerState = brokerState;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    @Override
    public String toString() {
        return "BrokerRegisterRequest{" +
                "groupName='" + groupName + '\'' +
                ", brokerRole=" + brokerRole +
                ", brokerState=" + brokerState +
                ", requestType=" + requestType +
                ", brokerAddress='" + brokerAddress + '\'' +
                '}';
    }
}
