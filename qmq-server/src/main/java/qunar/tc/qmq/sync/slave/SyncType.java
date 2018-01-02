package qunar.tc.qmq.sync.slave;

/**
 * @author yunfeng.yang
 * @since 2017/8/18
 */
public enum SyncType {
    action(1), message(2), heartbeat(3);

    private int code;

    SyncType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
