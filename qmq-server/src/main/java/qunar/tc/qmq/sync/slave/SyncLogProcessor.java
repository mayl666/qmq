package qunar.tc.qmq.sync.slave;

import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.model.SyncRequest;
import qunar.tc.qmq.protocol.Datagram;

/**
 * @author yunfeng.yang
 * @since 2017/8/18
 */
public interface SyncLogProcessor extends Disposable {
    void process(Datagram syncData);

    SyncRequest getRequest();

    boolean isRunning();
}
