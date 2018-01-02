package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.AbstractFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.metrics.Metrics;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.common.ClientDataStore;
import qunar.tc.qmq.config.PullSubjectsConfig;
import qunar.tc.qmq.consumer.pull.exception.PullException;
import qunar.tc.qmq.consumer.pull.model.AckSendInfo;
import qunar.tc.qmq.consumer.pull.model.PullParam;
import qunar.tc.qmq.consumer.pull.model.PullResult;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.netty.client.ResponseFuture;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.QMQSerializer;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.consumer.PullRequestPayloadHolder;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author yiqun.fan create on 17-8-17.
 */
class PullService {
    private static final Logger LOGGER = LoggerFactory.getLogger(PullService.class);
    private static final long MIN_RESPONSE_TIMEOUT = 4000;

    private static final Supplier<PullService> SUPPLIER = Suppliers.memoize(new Supplier<PullService>() {
        @Override
        public PullService get() {
            return new PullService();
        }
    });

    static PullService getInstance() {
        return SUPPLIER.get();
    }

    private PullService() {
    }

    private final NettyClient client = NettyClient.getClient();

    PullParam buildPullParam(BrokerGroupInfo brokerGroup, AckSendInfo ackSendInfo, int batchSize, int timeoutMillis, boolean isBroadcast) {
        PullParam pullParam = new PullParam();
        pullParam.setBrokerGroup(brokerGroup);
        pullParam.setPullBatchSize(batchSize);
        pullParam.setTimeoutMillis(timeoutMillis);
        pullParam.setConsumeOffset(-1);
        pullParam.setMinPullOffset(ackSendInfo.getMinPullOffset());
        pullParam.setMaxPullOffset(ackSendInfo.getMaxPullOffset());
        pullParam.setConsumerId(ClientDataStore.getClientId());
        pullParam.setBroadcast(isBroadcast);
        return pullParam;
    }

    Future<PullResult> pull(final String subject, final String group, PullParam pullParam) {
        PullRequest request = buildPullRequest(subject, group, pullParam);
        PullResultFuture result = new PullResultFuture(pullParam.getBrokerGroup());
        Datagram datagram = RemotingBuilder.buildRequestDatagram(CommandCode.PULL_MESSAGE, new PullRequestPayloadHolder(request));
        long responseTimeout = Math.max(2 * request.getTimeoutMillis(), MIN_RESPONSE_TIMEOUT);

        try {
            client.sendAsync(pullParam.getBrokerGroup().getMaster(), datagram, responseTimeout, new PullResponseCallback(request, result));
        } catch (ClientSendException e) {
            LOGGER.error("pull error. subject={}, group={}, errorCode={}", subject, group, e);
            monitorPullError(subject, group, e.getSendErrorCode().ordinal());
            result.setException(e);
        } catch (Exception e) {
            LOGGER.error("pull error. subject={}, group={}", subject, group, e);
            monitorPullError(subject, group, -1);
            result.setException(e);
        }
        return result;
    }

    private PullRequest buildPullRequest(String subject, String group, PullParam pullParam) {
        PullRequest request = new PullRequest();
        request.setSubject(subject);
        request.setGroup(group);
        request.setRequestNum(pullParam.getPullBatchSize());
        request.setTimeoutMillis(pullParam.getTimeoutMillis());
        request.setOffset(pullParam.getConsumeOffset());
        request.setPullOffsetBegin(pullParam.getMinPullOffset());
        request.setPullOffsetLast(pullParam.getMaxPullOffset());
        request.setConsumerId(pullParam.getConsumerId());
        request.setBroadcast(pullParam.isBroadcast());
        return request;
    }

    private static final class PullResponseCallback implements ResponseFuture.Callback {
        private final PullRequest request;
        private final PullResultFuture result;

        PullResponseCallback(PullRequest request, PullResultFuture result) {
            this.request = request;
            this.result = result;
        }

        @Override
        public void processResponse(ResponseFuture responseFuture) {
            monitorPullTime(request.getSubject(), request.getGroup(), responseFuture.getReceiveTime() - responseFuture.getBeginTime());

            final Datagram response = responseFuture.getResponse();
            if (!responseFuture.isSendOk() || response == null) {
                monitorPullError(request.getSubject(), request.getGroup(), -1);
                result.setException(new PullException("send fail"));
                return;
            }
            final short responseCode = response.getHeader().getCode();
            if (responseCode == CommandCode.NO_MESSAGE) {
                result.set(responseCode, Collections.<BaseMessage>emptyList());
            } else if (responseCode != CommandCode.SUCCESS) {
                monitorPullError(request.getSubject(), request.getGroup(), 100 + responseCode);
                result.set(responseCode, Collections.<BaseMessage>emptyList());
            } else {
                List<BaseMessage> messages = QMQSerializer.deserializeBaseMessage(response.getBody());
                if (messages == null) {
                    messages = Collections.emptyList();
                }
                monitorPullCount(request.getSubject(), request.getGroup(), messages.size());
                for (BaseMessage message : messages) {
                    message.setProperty(BaseMessage.keys.qmq_prefix, message.getSubject());
                    message.setProperty(BaseMessage.keys.qmq_consumerGroupName, request.getGroup());
                    if (message.getMaxRetryNum() < 0) {
                        String realSubject = RetrySubjectUtils.getRealSubject(message.getSubject());
                        message.setMaxRetryNum(PullSubjectsConfig.get().getMaxRetryNum(realSubject).get());
                    }
                }
                result.set(responseCode, messages);
            }
        }
    }

    private static final class PullResultFuture extends AbstractFuture<PullResult> {
        private final BrokerGroupInfo brokerGroup;

        PullResultFuture(BrokerGroupInfo brokerGroup) {
            this.brokerGroup = brokerGroup;
        }

        void set(short responseCode, List<BaseMessage> messages) {
            super.set(new PullResult(responseCode, messages, brokerGroup));
        }

        void setException(Exception ex) {
            super.setException(ex);
        }
    }

    private static void monitorPullTime(String subject, String group, long time) {
        Metrics.timer("qmq_pull_timer").tag("subject", subject).tag("group", group).get().update(time, TimeUnit.MILLISECONDS);
    }

    private static void monitorPullError(String subject, String group, int errorCode) {
        Metrics.counter("qmq_pull_error").tag("subject", subject).tag("group", group).tag("error", String.valueOf(errorCode)).delta().get().inc();
    }

    private static void monitorPullCount(String subject, String group, int pullSize) {
        Metrics.counter("qmq_pull_count").tag("subject", subject).tag("group", group).delta().get().inc(pullSize);
    }
}
