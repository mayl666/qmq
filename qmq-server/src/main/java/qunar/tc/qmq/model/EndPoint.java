package qunar.tc.qmq.model;

/**
 * @author yunfeng.yang
 * @since 2017/8/3
 */
public class EndPoint {
    private final String address;
    private final int port;

    public EndPoint(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }
}
