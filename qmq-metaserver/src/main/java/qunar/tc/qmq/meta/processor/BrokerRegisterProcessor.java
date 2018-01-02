package qunar.tc.qmq.meta.processor;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.*;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.connection.ConnectionManager;
import qunar.tc.qmq.meta.monitor.QMon;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.RemotingCommand;
import qunar.tc.qmq.serializer.Serializer;
import qunar.tc.qmq.serializer.SerializerFactory;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.List;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public class BrokerRegisterProcessor implements NettyRequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(BrokerRegisterProcessor.class);

    private static final Serializer SERIALIZER = SerializerFactory.create();
    private final CachedMetaInfoManager cachedMetaInfoManager;
    private final ConnectionManager connectionManager;
    private final Store store;

    public BrokerRegisterProcessor(CachedMetaInfoManager cachedMetaInfoManager, ConnectionManager connectionManager, Store store) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.connectionManager = connectionManager;
        this.store = store;
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        final ByteBuf body = request.getBody();
        final BrokerRegisterRequest brokerRequest = deserialize(body);
        final String groupName = brokerRequest.getGroupName();
        final int brokerRole = brokerRequest.getBrokerRole();
        final int requestType = brokerRequest.getRequestType();
        final int brokerState = brokerRequest.getBrokerState();
        final String brokerAddress = brokerRequest.getBrokerAddress();

        QMon.brokerRegisterCountInc(groupName, requestType);

        LOG.info("broker register request received. request: {}", brokerRequest);

        // slave 暂不处理
        if (brokerRole == BrokerRole.SLAVE.getCode()) {
            final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS,
                    request.getOpaque(), null);
            ctx.writeAndFlush(datagram);
            return;
        }

        connectionManager.putBrokerConnection(ctx.channel(), groupName);

        if (requestType == BrokerRequestType.HEARTBEAT.getCode()) {
            final BrokerGroup brokerGroupInCache = cachedMetaInfoManager.getBrokerGroup(groupName);
            if (brokerGroupInCache == null || brokerState != brokerGroupInCache.getBrokerState().getCode()) {
                final BrokerGroup groupInStore = store.getBrokerGroup(groupName);
                if (groupInStore != null && groupInStore.getBrokerState().getCode() != brokerState) {
                    store.updateBrokerGroup(groupName, BrokerState.codeOf(brokerState));
                    cachedMetaInfoManager.executeRefreshTask();
                }
            }
            final List<String> subjects = cachedMetaInfoManager.getSubjects(groupName);
            LOG.info("Broker heartbeat response, request:{}, subjects:{}", brokerRequest, subjects);
            final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS,
                    request.getHeader().getOpaque(), new BrokerRegisterResponsePayloadHolder(subjects));
            ctx.writeAndFlush(datagram);
        }

        if (requestType == BrokerRequestType.ONLINE.getCode()) {
            store.insertOrUpdateBrokerGroup(groupName, brokerAddress, BrokerState.RW);
            cachedMetaInfoManager.executeRefreshTask();
            final List<String> subjects = cachedMetaInfoManager.getSubjects(groupName);
            LOG.info("Broker online success, request:{}, subjects:{}", brokerRequest, subjects);
            final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS,
                    request.getHeader().getOpaque(), new BrokerRegisterResponsePayloadHolder(subjects));
            ctx.writeAndFlush(datagram);
        }

        if (requestType == BrokerRequestType.OFFLINE.getCode()) {
            store.insertOrUpdateBrokerGroup(groupName, brokerAddress, BrokerState.NRW);
            cachedMetaInfoManager.executeRefreshTask();
            LOG.info("broker offline success, request:{}", brokerRequest);
            final Datagram datagram = RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS,
                    request.getOpaque(), null);
            ctx.writeAndFlush(datagram);
        }
    }

    private static class BrokerRegisterResponsePayloadHolder implements PayloadHolder {
        private final BrokerRegisterResponse response;

        private BrokerRegisterResponsePayloadHolder(List<String> subjects) {
            this.response = new BrokerRegisterResponse();
            this.response.setSubjects(subjects);
        }

        @Override
        public void writeBody(ByteBuf out) {
            final byte[] bytes = SERIALIZER.serializeToBytes(response);
            out.writeBytes(bytes);
        }
    }

    private BrokerRegisterRequest deserialize(ByteBuf buf) {
        final byte[] bytes = new byte[buf.readableBytes()];
        final int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        return SERIALIZER.deSerialize(bytes, BrokerRegisterRequest.class);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
