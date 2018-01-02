package qunar.tc.qmq.sync.master;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.Config;
import qunar.tc.qmq.model.SyncRequest;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.store.SelectSegmentBufferResult;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
class MessageLogSyncWorker extends AbstractLogSyncWorker implements Disposable {
    private final AsyncEventBus messageLogSyncEventBus;
    private final ExecutorService dispatchExecutor;

    MessageLogSyncWorker(MessageStoreWrapper messageStoreWrapper, Config config) {
        super(messageStoreWrapper, config);
        this.dispatchExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("heart-event-bus-%d").build());
        this.messageLogSyncEventBus = new AsyncEventBus(dispatchExecutor);
    }

    @Override
    protected SyncLog getSyncLog(SyncRequest syncRequest) {
        messageLogSyncEventBus.post(syncRequest);
        final SelectSegmentBufferResult result = messageStoreWrapper.getMessageLogs(syncRequest.getMessageLogOffset());
        if (result == null || result.getSize() == 0) {
            return new SyncLog(syncRequest.getMessageLogOffset(), null, false, 0);
        }

        return new SyncLog(result.getStartOffset(), result.getBuffer(), true, result.getSize());
    }

    void registerSyncEvent(Object listener) {
        messageLogSyncEventBus.register(listener);
    }

    @Override
    public void destroy() {
        if (dispatchExecutor != null) {
            dispatchExecutor.shutdown();
        }
    }
}
