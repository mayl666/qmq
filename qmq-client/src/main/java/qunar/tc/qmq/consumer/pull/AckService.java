package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.metrics.Counter;
import qunar.metrics.Metrics;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.ClientDataStore;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.MapKeyBuilder;
import qunar.tc.qmq.common.QmqLogger;
import qunar.tc.qmq.consumer.pull.exception.AckException;
import qunar.tc.qmq.consumer.pull.model.AckEntry;
import qunar.tc.qmq.consumer.pull.model.AckSendEntry;
import qunar.tc.qmq.consumer.pull.model.AckSendInfo;
import qunar.tc.qmq.consumer.pull.model.PullResult;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.client.ResponseFuture;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.consumer.AckRequest;
import qunar.tc.qmq.protocol.consumer.AckRequestPayloadHolder;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author yiqun.fan create on 17-8-18.
 */
class AckService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AckService.class);

    private static final Supplier<AckService> SUPPLIER = Suppliers.memoize(new Supplier<AckService>() {
        @Override
        public AckService get() {
            return new AckService();
        }
    });

    static AckService getInstance() {
        return SUPPLIER.get();
    }

    private AckService() {
    }

    private static final Counter GET_PULL_OFFSET_ERROR = Metrics.counter("qmq_pull_getpulloffset_error").delta().get();
    private static final long ACK_REQUEST_TIMEOUT_MILLIS = 5000;  // 5秒
    private static final HashedWheelTimer TIMER = new HashedWheelTimer();
    private final NettyClient client = NettyClient.getClient();
    private final ConcurrentMap<String, AckSendQueue> senderMap = new ConcurrentHashMap<>();

    List<PulledMessage> convertToPulledMessage(String subject, String group, PullResult pullResult, AckHook ackHook) {
        final List<BaseMessage> baseMessages = pullResult.getMessages();
        final List<PulledMessage> messages = new ArrayList<>(baseMessages.size());
        final List<AckEntry> ackEntries = new ArrayList<>(baseMessages.size());
        final AckSendQueue sendQueue = getOrCreateSendQueue(pullResult.getBrokerGroup(), subject, group);
        long prevPullOffset = 0, minPullOffset = Long.MAX_VALUE, maxPullOffset = 0;
        for (BaseMessage baseMessage : baseMessages) {
            final long pullOffset = getOffset(baseMessage, BaseMessage.keys.qmq_pullOffset);
            if (pullOffset < prevPullOffset) {
                monitorGetPullOffsetError(baseMessage);
                continue;
            }
            prevPullOffset = pullOffset;
            minPullOffset = Math.min(minPullOffset, pullOffset);
            maxPullOffset = Math.max(maxPullOffset, pullOffset);
            AckEntry ackEntry = new AckEntry(sendQueue, pullOffset);
            ackEntries.add(ackEntry);
            messages.add(new PulledMessage(baseMessage, ackEntry, ackHook));
        }
        for (int i = 0; i < ackEntries.size() - 1; i++) {
            ackEntries.get(i).setNext(ackEntries.get(i + 1));
        }
        sendQueue.appendAckEntries(ackEntries);
        return messages;
    }

    private AckSendQueue getOrCreateSendQueue(BrokerGroupInfo brokerGroup, String subject, String group) {
        final String senderKey = MapKeyBuilder.buildSenderKey(brokerGroup.getGroupName(), subject, group);
        AckSendQueue sender = senderMap.get(senderKey);
        if (sender == null) {
            sender = new AckSendQueue(brokerGroup.getGroupName(), subject, group, TIMER);
            AckSendQueue old = senderMap.putIfAbsent(senderKey, sender);
            return old != null ? old : sender;
        }
        return sender;
    }

    private long getOffset(BaseMessage message, BaseMessage.keys keys) {
        Object offsetObj = message.getProperty(keys);
        if (offsetObj == null) {
            return -1;
        }
        try {
            return Long.parseLong(offsetObj.toString());
        } catch (Exception e) {
            return -1;
        }
    }

    private static void monitorGetPullOffsetError(BaseMessage message) {
        QmqLogger.log("lost pull offset. msgId=" + message.getMessageId());
        GET_PULL_OFFSET_ERROR.inc();
    }

    void sendAck(BrokerGroupInfo brokerGroup, String subject, String group, AckSendEntry ack, SendAckCallback callback) {
        AckRequest request = buildAckRequest(subject, group, ack);
        Datagram datagram = RemotingBuilder.buildRequestDatagram(CommandCode.ACK_REQUEST, new AckRequestPayloadHolder(request));
        sendRequest(brokerGroup, subject, group, request, datagram, callback);
    }

    private AckRequest buildAckRequest(String subject, String group, AckSendEntry ack) {
        AckRequest request = new AckRequest();
        request.setSubject(subject);
        request.setGroup(group);
        request.setConsumerId(ClientDataStore.getClientId());
        request.setPullOffsetBegin(ack.getPullOffsetBegin());
        request.setPullOffsetLast(ack.getPullOffsetLast());
        return request;
    }

    private void sendRequest(BrokerGroupInfo brokerGroup, String subject, String group, AckRequest request, Datagram datagram, SendAckCallback callback) {
        try {
            client.sendAsync(brokerGroup.getMaster(), datagram, ACK_REQUEST_TIMEOUT_MILLIS, new AckResponseCallback(request, callback));
        } catch (ClientSendException e) {
            ClientSendException.SendErrorCode errorCode = e.getSendErrorCode();
            monitorAckError(subject, group, errorCode.ordinal());
            callback.fail(e);
        } catch (Exception e) {
            monitorAckError(subject, group, -1);
            callback.fail(e);
        }
    }

    private static final class AckResponseCallback implements ResponseFuture.Callback {
        private final AckRequest request;
        private final SendAckCallback sendAckCallback;

        AckResponseCallback(AckRequest request, SendAckCallback sendAckCallback) {
            this.request = request;
            this.sendAckCallback = sendAckCallback;
        }

        @Override
        public void processResponse(ResponseFuture responseFuture) {
            monitorAckTime(request.getSubject(), request.getGroup(), responseFuture.getReceiveTime() - responseFuture.getBeginTime());

            Datagram response = responseFuture.getResponse();
            if (!responseFuture.isSendOk() || response == null) {
                monitorAckError(request.getSubject(), request.getGroup(), -1);
                sendAckCallback.fail(new AckException("send fail"));
                BrokerServiceImpl.getInstance().refresh(ClientType.CONSUMER, request.getSubject());
                return;
            }
            final short responseCode = response.getHeader().getCode();
            if (responseCode == CommandCode.SUCCESS) {
                sendAckCallback.success();
            } else {
                monitorAckError(request.getSubject(), request.getGroup(), 100 + responseCode);
                BrokerServiceImpl.getInstance().refresh(ClientType.CONSUMER, request.getSubject());
                sendAckCallback.fail(new AckException("responseCode: " + responseCode));
            }
        }
    }

    private static void monitorAckTime(String subject, String group, long time) {
        Metrics.timer("qmq_pull_ack_timer").tag("subject", subject).tag("group", group).get().update(time, TimeUnit.MILLISECONDS);
    }

    private static void monitorAckError(String subject, String group, int errorCode) {
        LOGGER.error("ack error. subject={}, group={}, errorCode={}", subject, group, errorCode);
        Metrics.counter("qmq_pull_ack_error").tag("subject", subject).tag("group", group).tag("error", String.valueOf(errorCode)).delta().get().inc();
    }

    AckSendInfo getAckSendInfo(BrokerGroupInfo brokerGroup, String subject, String group) {
        AckSendQueue sender = senderMap.get(MapKeyBuilder.buildSenderKey(brokerGroup.getGroupName(), subject, group));
        return sender != null ? sender.getAckSendInfo() : new AckSendInfo();
    }

    interface SendAckCallback {
        void success();

        void fail(Exception ex);
    }
}
