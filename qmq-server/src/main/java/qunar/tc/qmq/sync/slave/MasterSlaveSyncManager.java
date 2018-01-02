package qunar.tc.qmq.sync.slave;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.consumer.ActionLogManagerImpl;
import qunar.tc.qmq.model.SyncRequest;
import qunar.tc.qmq.netty.NettyClientConfig;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.netty.exception.RemoteTimeoutException;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author yunfeng.yang
 * @since 2017/8/18
 */
public class MasterSlaveSyncManager implements Disposable {
    private static final Logger logger = LoggerFactory.getLogger(ActionLogManagerImpl.class);
    private static final ExecutorService SYNC_POOL = Executors.newFixedThreadPool(3,
            new ThreadFactoryBuilder().setNameFormat("slave-sync-%d").build());

    private final String masterAddress;
    private final SlaveSyncSender slaveSyncSender;
    private final SlaveSyncTask messageSyncTask;
    private final SlaveSyncTask actionSyncTask;
    private final SlaveSyncTask heartbeatTask;

    public MasterSlaveSyncManager(final String masterAddress, final MessageStoreWrapper messageStoreWrapper) {
        final NettyClient client = NettyClient.getClient();
        client.start(new NettyClientConfig());
        this.slaveSyncSender = new SlaveSyncSender(client);
        this.masterAddress = masterAddress;

        final SyncLogProcessor messageSyncLogProcessor = new SyncMessageLogProcessor(messageStoreWrapper);
        messageSyncTask = new SlaveSyncTask(messageSyncLogProcessor);
        final SyncLogProcessor actionSyncLogProcessor = new SyncActionLogProcessor(messageStoreWrapper);
        actionSyncTask = new SlaveSyncTask(actionSyncLogProcessor);
        final SyncLogProcessor heartbeatProcessor = new HeartBeatProcessor(messageStoreWrapper);
        heartbeatTask = new SlaveSyncTask(heartbeatProcessor);

        startSync();
    }

    private void startSync() {
        SYNC_POOL.submit(messageSyncTask);
        SYNC_POOL.submit(actionSyncTask);
        SYNC_POOL.submit(heartbeatTask);
    }

    @Override
    public void destroy() {
        if (SYNC_POOL != null) {
            SYNC_POOL.shutdown();
        }
    }

    private class SlaveSyncTask implements Runnable {
        private final SyncLogProcessor syncLogProcessor;

        SlaveSyncTask(SyncLogProcessor syncLogProcessor) {
            this.syncLogProcessor = syncLogProcessor;
        }

        @Override
        public void run() {
            while (syncLogProcessor.isRunning()) {
                try {
                    final SyncRequest syncRequest = syncLogProcessor.getRequest();
                    final SyncRequestPayloadHolder syncRequestPayloadHolder = new SyncRequestPayloadHolder(syncRequest);
                    final Datagram request = RemotingBuilder.buildRequestDatagram(CommandCode.SYNC_REQUEST, syncRequestPayloadHolder);

                    syncDataFromMaster(request);
                } catch (InterruptedException | RemoteTimeoutException | ClientSendException e) {
                    logger.error("sync data from master error", e);
                }
            }
        }

        private void syncDataFromMaster(Datagram request) throws InterruptedException, RemoteTimeoutException, ClientSendException {
            Datagram response = null;
            try {
                response = slaveSyncSender.send(masterAddress, request);
                syncLogProcessor.process(response);
            } finally {
                if (response != null) {
                    response.release();
                }
            }
        }
    }

    private class SyncRequestPayloadHolder implements PayloadHolder {
        private final SyncRequest syncRequest;

        SyncRequestPayloadHolder(SyncRequest syncRequest) {
            this.syncRequest = syncRequest;
        }

        @Override
        public void writeBody(ByteBuf out) {
            out.writeByte(syncRequest.getSyncType());
            out.writeLong(syncRequest.getMessageLogOffset());
            out.writeLong(syncRequest.getActionLogOffset());
        }
    }
}
