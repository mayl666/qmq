package qunar.tc.qmq.sync.slave;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.model.SyncRequest;
import qunar.tc.qmq.store.MessageStoreWrapper;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public class SyncActionLogProcessor extends AbstractSyncLogProcessor {

    SyncActionLogProcessor(MessageStoreWrapper messageStoreWrapper) {
        super(messageStoreWrapper);
    }

    @Override
    void appendLogs(long startOffset, ByteBuf body) {
        messageStoreWrapper.appendActionLogs(startOffset, body);
    }

    @Override
    public SyncRequest getRequest() {
        long actionLogMaxOffset = messageStoreWrapper.getActionLogMaxOffset();
        return new SyncRequest(SyncType.action.getCode(), 0, actionLogMaxOffset);
    }
}