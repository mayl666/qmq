package qunar.tc.qmq.consumer.pull.impl;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.metrics.Metrics;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.consumer.pull.SendMessageBack;
import qunar.tc.qmq.consumer.pull.exception.SendMessageBackException;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.client.ResponseFuture;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.MessagesPayloadHolder;
import qunar.tc.qmq.protocol.QMQSerializer;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;
import qunar.tc.qmq.protocol.producer.SendResult;
import qunar.tc.qmq.util.RemotingBuilder;

import java.util.Map;

/**
 * @author yiqun.fan create on 17-8-23.
 */
public class SendMessageBackImpl implements SendMessageBack {
    private static final Logger LOGGER = LoggerFactory.getLogger(SendMessageBackImpl.class);
    private static final long SEND_MESSAGE_TIMEOUT = 5000;  // 5秒

    private static final NettyClient NETTY_CLIENT = NettyClient.getClient();

    private static final Supplier<SendMessageBackImpl> SUPPLIER = Suppliers.memoize(new Supplier<SendMessageBackImpl>() {
        @Override
        public SendMessageBackImpl get() {
            return new SendMessageBackImpl();
        }
    });

    public static SendMessageBackImpl getInstance() {
        return SUPPLIER.get();
    }

    private final BrokerService brokerService = BrokerServiceImpl.getInstance();

    @Override
    public void sendBack(final BrokerGroupInfo brokerGroup, BaseMessage messages, final Callback callback) {
        if (messages == null) {
            if (callback != null) {
                callback.success();
            }
            return;
        }
        final String subject = messages.getSubject();
        Datagram datagram = RemotingBuilder.buildRequestDatagram(CommandCode.SEND_MESSAGE, new MessagesPayloadHolder(Lists.newArrayList(messages)));
        try {
            NETTY_CLIENT.sendAsync(brokerGroup.getMaster(), datagram, SEND_MESSAGE_TIMEOUT,
                    new ResponseFuture.Callback() {
                        @Override
                        public void processResponse(ResponseFuture responseFuture) {
                            final Datagram response = responseFuture.getResponse();
                            if (!responseFuture.isSendOk() || response == null) {
                                monitorSendError(subject, -1);
                                callback.fail(new SendMessageBackException("send fail"));
                            } else {
                                final int respCode = response.getHeader().getCode();
                                final SendResult sendResult = getSendResult(response);
                                if (sendResult == null) {
                                    monitorSendError(subject, 100 + respCode);
                                    callback.fail(new SendMessageBackException("responseCode=" + respCode));
                                    return;
                                }
                                if (respCode == CommandCode.SUCCESS && sendResult.getCode() == MessageProducerCode.SUCCESS) {
                                    callback.success();
                                    return;
                                }
                                if (respCode == CommandCode.BROKER_REJECT || sendResult.getCode() == MessageProducerCode.BROKER_READ_ONLY) {
                                    brokerGroup.setAvailable(false);
                                    brokerService.refresh(ClientType.PRODUCER, subject);
                                }
                                monitorSendError(subject, 100 + respCode);
                                callback.fail(new SendMessageBackException("responseCode=" + respCode + ", sendCode=" + sendResult.getCode()));
                            }
                        }
                    });
        } catch (ClientSendException e) {
            LOGGER.error("send message error. subject={}", subject);
            monitorSendError(subject, e.getSendErrorCode().ordinal());
            callback.fail(e);
        } catch (Exception e) {
            LOGGER.error("send message error. subject={}", subject);
            monitorSendError(subject, -1);
            callback.fail(e);
        }
    }

    private SendResult getSendResult(Datagram response) {
        try {
            Map<String, SendResult> result = QMQSerializer.deserializeSendResultMap(response.getBody());
            return result.values().iterator().next();
        } catch (Exception e) {
            LOGGER.error("sendback exception on deserializeSendResultMap.", e);
            return null;
        }
    }

    private static void monitorSendError(String subject, int errorCode) {
        Metrics.counter("qmq_pull_send_msg_error").tag("subject", subject).tag("error", String.valueOf(errorCode)).delta().get().inc();
    }
}
