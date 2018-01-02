package qunar.tc.qmq.sync.master;

import qunar.tc.qmq.configuration.Config;
import qunar.tc.qmq.model.SyncRequest;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.store.SelectSegmentBufferResult;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
class ActionLogSyncWorker extends AbstractLogSyncWorker {

    ActionLogSyncWorker(MessageStoreWrapper messageStoreWrapper, Config config) {
        super(messageStoreWrapper, config);
    }

    @Override
    protected SyncLog getSyncLog(SyncRequest syncRequest) {
        final SelectSegmentBufferResult result = messageStoreWrapper.getActionLogs(syncRequest.getActionLogOffset());
        if (result == null || result.getSize() == 0) {
            return new SyncLog(syncRequest.getMessageLogOffset(), null, false, 0);
        }
        return new SyncLog(result.getStartOffset(), result.getBuffer(), true, result.getSize());
    }
}
