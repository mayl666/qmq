package qunar.tc.qmq.producer.sender;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.metrics.Metrics;
import qunar.metrics.Timer;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerLoadBalance;
import qunar.tc.qmq.broker.impl.BrokerLoadBalanceImpl;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.netty.exception.*;
import qunar.tc.qmq.protocol.*;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;
import qunar.tc.qmq.protocol.producer.SendResult;
import qunar.tc.qmq.service.exceptions.BlockMessageException;
import qunar.tc.qmq.service.exceptions.DuplicateMessageException;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.List;
import java.util.Map;

/**
 * @author zhenyu.nie created on 2017 2017/7/5 15:08
 */
class NettyConnection implements Connection {
    private static final Logger logger = LoggerFactory.getLogger(NettyConnection.class);

    private final String subject;
    private final NettyProducerClient producerClient;
    private final BrokerClusterInfo brokerCluster;
    private final BrokerLoadBalance brokerLoadBalance = BrokerLoadBalanceImpl.getInstance();
    private volatile BrokerGroupInfo sendBrokerGroup = null;

    private final String url;

    NettyConnection(String subject, NettyProducerClient producerClient, BrokerClusterInfo brokerCluster) {
        this.url = "newqmq://" + subject;
        this.subject = subject;
        this.producerClient = producerClient;
        this.brokerCluster = brokerCluster;
    }

    @Override
    public void preHeat() {
    }

    @Override
    public String url() {
        return url;
    }

    @Override
    public Map<String, MessageException> send(List<ProduceMessage> messages) throws ClientSendException, RemoteException, BrokerRejectException {
        long start = System.currentTimeMillis();

        for (ProduceMessage message : messages) {
            Metrics.meter("newqmq.producer.send.meter").tag("subject", message.getSubject()).get().mark();
        }

        Timer.Context context = Metrics.timer("newqmq.producer.batch.send.timer").get().time();
        sendBrokerGroup = brokerLoadBalance.loadBalance(brokerCluster, sendBrokerGroup);
        Datagram response;
        try {
            response = doSend(sendBrokerGroup.getMaster(), messages);
        } catch (Exception e) {
            Metrics.counter("newqmq.producer.send.fail.counter").delta().get().inc(messages.size());
            throw e;
        } finally {
            context.stop();
        }

        logger.debug("newqmq producer send time: {}, size {}", System.currentTimeMillis() - start, messages.size());
        RemotingHeader responseHeader = response.getHeader();
        int code = responseHeader.getCode();
        switch (code) {
            case CommandCode.SUCCESS:
                return process(response);
            case CommandCode.BROKER_REJECT:
                handleSendReject();
                throw new BrokerRejectException("");
            default:
                throw new RemoteException();
        }
    }

    public void handleSendReject() {
        if (sendBrokerGroup != null) {
            sendBrokerGroup.setAvailable(false);
        }
        BrokerServiceImpl.getInstance().refresh(ClientType.PRODUCER, subject);
    }

    private Map<String, MessageException> process(Datagram response) throws RemoteResponseUnreadableException {
        ByteBuf buf = response.getBody();
        try {
            if (buf == null || buf.readableBytes() == 0) {
                logger.warn("response body is null");
                // todo:
                return null;
            }

            Map<String, SendResult> resultMap = QMQSerializer.deserializeSendResultMap(buf);
            if (resultMap == null) {
                throw new RemoteResponseUnreadableException();
            }

            boolean brokerReject = false;
            Map<String, MessageException> map = Maps.newHashMapWithExpectedSize(resultMap.size());
            for (Map.Entry<String, SendResult> entry : resultMap.entrySet()) {
                String messageId = entry.getKey();
                SendResult result = entry.getValue();
                switch (result.getCode()) {
                    case MessageProducerCode.SUCCESS:
                        break;
                    case MessageProducerCode.MESSAGE_DUPLICATE:
                        map.put(messageId, new DuplicateMessageException(messageId));
                        break;
                    case MessageProducerCode.BROKER_BUSY:
                        map.put(messageId, new MessageException(messageId, MessageException.BROKER_BUSY));
                        break;
                    case MessageProducerCode.BROKER_READ_ONLY:
                        brokerReject = true;
                        map.put(messageId, new BrokerRejectException(messageId));
                        break;
                    case MessageProducerCode.SUBJECT_NOT_ASSIGNED:
                        map.put(messageId, new SubjectNotAssignedException(messageId));
                        break;
                    case MessageProducerCode.BLOCK:
                        map.put(messageId, new BlockMessageException(messageId));
                        break;
                    default:
                        map.put(messageId, new MessageException(messageId, result.getRemark()));
                        break;
                }
            }

            if (brokerReject) {
                handleSendReject();
            }
            return map;
        } finally {
            if (buf != null) {
                buf.release();
            }
        }
    }

    private Datagram doSend(String address, List<ProduceMessage> messages) throws ClientSendException, RemoteTimeoutException {
        try {
            Datagram datagram = buildDataGram(messages);
            return producerClient.sendMessage(address, datagram);
        } catch (InterruptedException e) {
            logger.warn("send request be interrupted", e);
            throw new RuntimeException(e);
        }
    }

    private Datagram buildDataGram(List<ProduceMessage> messages) {
        RemotingHeader remotingHeader = new RemotingHeader();
        remotingHeader.setMagicCode(RemotingHeader.DEFAULT_MAGIC_CODE);
        remotingHeader.setVersion(RemotingHeader.VERSION_1);
        remotingHeader.setCode(CommandCode.SEND_MESSAGE);

        Datagram datagram = new Datagram();
        datagram.setHeader(remotingHeader);

        List<BaseMessage> baseMessages = Lists.newArrayListWithCapacity(messages.size());
        for (ProduceMessage message : messages) {
            baseMessages.add((BaseMessage) message.getBase());
        }
        datagram.setPayloadHolder(new MessagesPayloadHolder(baseMessages));
        return datagram;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NettyConnection that = (NettyConnection) o;
        return Objects.equal(subject, that.subject);
    }

    @Override
    public int hashCode() {
        return subject.hashCode();
    }

    @Override
    public void destroy() {
    }
}
