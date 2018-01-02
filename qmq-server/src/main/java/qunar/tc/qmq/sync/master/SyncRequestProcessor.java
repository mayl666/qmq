package qunar.tc.qmq.sync.master;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BrokerRole;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.Config;
import qunar.tc.qmq.model.SyncRequest;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.sync.slave.SyncType;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author yunfeng.yang
 * @since 2017/8/19
 */
public class SyncRequestProcessor implements NettyRequestProcessor, Disposable {
    private static final Logger logger = LoggerFactory.getLogger(SyncRequestProcessor.class);
    private static final ExecutorService SYNC_PROCESS_POOL = Executors.newFixedThreadPool(3,
            new ThreadFactoryBuilder().setNameFormat("master-sync-%d").build());
    static final HashedWheelTimer TIMER = new HashedWheelTimer(1, TimeUnit.MILLISECONDS);

    private final MessageLogSyncWorker messageLogSyncWorker;

    static {
        TIMER.start();
    }

    private final Map<Integer, SyncProcessor> processorMap;

    SyncRequestProcessor(MessageStoreWrapper messageStoreWrapper, Config config) {
        this.processorMap = new HashMap<>();
        this.messageLogSyncWorker = new MessageLogSyncWorker(messageStoreWrapper, config);
        final ActionLogSyncWorker ackLogSyncWorker = new ActionLogSyncWorker(messageStoreWrapper, config);
        final HeartbeatSyncWorker heartBeatSyncWorker = new HeartbeatSyncWorker(messageStoreWrapper);
        processorMap.put(SyncType.message.getCode(), messageLogSyncWorker);
        processorMap.put(SyncType.action.getCode(), ackLogSyncWorker);
        processorMap.put(SyncType.heartbeat.getCode(), heartBeatSyncWorker);
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        final int opaque = request.getHeader().getOpaque();
        final SyncRequest syncRequest = deserializeSyncRequest(request);
        final int logType = syncRequest.getSyncType();
        final SyncProcessor processor = processorMap.get(logType);
        if (processor == null) {
            logger.error("unknown log type {}", logType);
            ctx.writeAndFlush(RemotingBuilder.buildResponseDatagram(CommandCode.BROKER_ERROR, opaque, null));
            return;
        }
        final SyncRequestEntry syncRequestEntry = new SyncRequestEntry(ctx, syncRequest, opaque);
        SYNC_PROCESS_POOL.submit(new SyncRequestProcessTask(syncRequestEntry, processor));
    }

    @Override
    public boolean rejectRequest() {
        return BrokerConfig.getBrokerRole() == BrokerRole.SLAVE;
    }

    void registerSyncEvent(Object listener) {
        this.messageLogSyncWorker.registerSyncEvent(listener);
    }

    private SyncRequest deserializeSyncRequest(final RemotingCommand request) {
        final ByteBuf body = request.getBody();
        final int logType = body.readByte();
        final long messageLogOffset = body.readLong();
        final long actionLogOffset = body.readLong();
        return new SyncRequest(logType, messageLogOffset, actionLogOffset);
    }

    @Override
    public void destroy() {
        messageLogSyncWorker.destroy();
        if (SYNC_PROCESS_POOL != null) {
            SYNC_PROCESS_POOL.shutdown();
        }
    }

    static class SyncRequestTimeoutTask implements TimerTask {

        private final SyncRequestEntry entry;
        private final SyncProcessor processor;

        SyncRequestTimeoutTask(SyncRequestEntry entry, SyncProcessor processor) {
            this.entry = entry;
            this.processor = processor;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            try {
                processor.processTimeout(entry);
            } catch (Exception e) {
                logger.error("process sync request error", e);
            }
        }
    }

    private static class SyncRequestProcessTask implements Runnable {

        private final SyncRequestEntry entry;
        private final SyncProcessor processor;

        private SyncRequestProcessTask(SyncRequestEntry entry, SyncProcessor processor) {
            this.entry = entry;
            this.processor = processor;
        }

        @Override
        public void run() {
            try {
                processor.process(entry);
            } catch (Exception e) {
                logger.error("process sync request error", e);
            }
        }
    }

    static class SyncRequestEntry {
        private final ChannelHandlerContext ctx;
        private final SyncRequest syncRequest;
        private final int opaque;

        SyncRequestEntry(ChannelHandlerContext ctx, SyncRequest syncRequest, int opaque) {
            this.ctx = ctx;
            this.syncRequest = syncRequest;
            this.opaque = opaque;
        }

        ChannelHandlerContext getCtx() {
            return ctx;
        }

        SyncRequest getSyncRequest() {
            return syncRequest;
        }

        int getOpaque() {
            return opaque;
        }
    }

    interface SyncProcessor {
        void process(SyncRequestEntry entry);

        void processTimeout(SyncRequestEntry entry);
    }
}
