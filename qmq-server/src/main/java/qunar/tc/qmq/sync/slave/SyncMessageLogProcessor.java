package qunar.tc.qmq.sync.slave;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.model.SyncRequest;
import qunar.tc.qmq.store.MessageStoreWrapper;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public class SyncMessageLogProcessor extends AbstractSyncLogProcessor {

    SyncMessageLogProcessor(MessageStoreWrapper messageStoreWrapper) {
        super(messageStoreWrapper);
    }

    @Override
    void appendLogs(long startOffset, ByteBuf body) {
        messageStoreWrapper.appendMessageLogs(startOffset, body);
    }

    @Override
    public SyncRequest getRequest() {
        final long messageLogMaxOffset = messageStoreWrapper.getMessageLogMaxOffset();
        return new SyncRequest(SyncType.message.getCode(), messageLogMaxOffset, 0L);
    }
}
