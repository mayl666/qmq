package qunar.tc.qmq.processor;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Conf;
import qunar.agile.Numbers;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.base.MessageHeader;
import qunar.tc.qmq.base.RawMessage;
import qunar.tc.qmq.batch.Flusher;
import qunar.tc.qmq.batch.Processor;
import qunar.tc.qmq.configuration.Config;
import qunar.tc.qmq.model.ReceiveResult;
import qunar.tc.qmq.model.ReceivingMessage;
import qunar.tc.qmq.model.SyncRequest;
import qunar.tc.qmq.monitor.QMon;
import qunar.tc.qmq.processor.filters.ReceiveFilterChain;
import qunar.tc.qmq.protocol.CommandCode;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.producer.MessageProducerCode;
import qunar.tc.qmq.store.MessageStoreWrapper;
import qunar.tc.qmq.util.RemotingBuilder;
import qunar.tc.qmq.utils.CharsetUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * @author yunfeng.yang
 * @since 2017/8/7
 */
public class Receiver implements Processor<ReceivingMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(Receiver.class);

    private final Config config;
    private final Flusher<ReceivingMessage> bufferFlusher;
    private final Invoker invoker;
    private final MessageStoreWrapper messageStoreWrapper;
    private final SubjectWritableService subjectWritableService;
    private final ConcurrentSkipListMap<Long, ReceiveEntry> receiveEntries;
    private final AtomicLong receiveErrorIndex;

    public Receiver(final Config config, final MessageStoreWrapper messageStoreWrapper, SubjectWritableService subjectWritableService) {
        this.config = config;
        this.messageStoreWrapper = messageStoreWrapper;
        this.subjectWritableService = subjectWritableService;
        this.bufferFlusher = buildReceiverFlusher();
        this.invoker = new ReceiveFilterChain().buildFilterChain(this::doInvoke);
        this.receiveErrorIndex = new AtomicLong(0);
        this.receiveEntries = new ConcurrentSkipListMap<>();
    }

    ListenableFuture<Datagram> receive(final List<RawMessage> messages) {
        final List<SettableFuture<ReceiveResult>> futures = new ArrayList<>(messages.size());

        for (final RawMessage message : messages) {
            final MessageHeader header = message.getHeader();
            QMon.receivedMessagesCountInc(header.getSubject());
            QMon.produceTime(header.getSubject(), System.currentTimeMillis() - message.getHeader().getCreateTime());
            final ReceivingMessage receivingMessage = new ReceivingMessage(message, System.currentTimeMillis());
            futures.add(receivingMessage.promise());
            invoker.invoke(receivingMessage);
        }
        return Futures.transform(Futures.allAsList(futures), TRANSFORM::apply);
    }

    @Override
    public void process(List<ReceivingMessage> messages) {
        try {
            List<ReceiveResult> results = messageStoreWrapper.putMessages(messages);
            for (int i = 0; i < messages.size(); i++) {
                offer(messages.get(i), results.get(i));
            }
        } catch (Throwable t) {
            error(messages, t);
        }
    }

    public static class SendResultPayloadHolder implements PayloadHolder {
        private final List<ReceiveResult> results;

        SendResultPayloadHolder(List<ReceiveResult> results) {
            this.results = results;
        }

        @Override
        public void writeBody(ByteBuf out) {
            for (ReceiveResult receiveResult : results) {
                byte[] messageIdBytes = CharsetUtils.toUTF8Bytes(receiveResult.getMessageId());
                out.writeShort((short) messageIdBytes.length);
                out.writeBytes(messageIdBytes);

                int code = receiveResult.getCode();
                out.writeInt(code);

                byte[] remarkBytes = CharsetUtils.toUTF8Bytes(receiveResult.getRemark());
                out.writeShort((short) remarkBytes.length);
                out.writeBytes(remarkBytes);
            }
        }
    }

    private static final Function<List<ReceiveResult>, Datagram> TRANSFORM = receiveResults -> RemotingBuilder.buildResponseDatagram(CommandCode.SUCCESS, 0, new SendResultPayloadHolder(receiveResults));

    private void doInvoke(ReceivingMessage message) {
        if (subjectWritableService.isReadonly()) {
            brokerReadOnly(message);
            return;
        }
        if (allowReceiverReject()) {
            if (!subjectWritableService.isSubjectWritable(message.getSubject())) {
                notAllowed(message);
                return;
            }
        }

        if (config.getBoolean("receiver.flusher.enable", false)) {
            if (!bufferFlusher.add(message)) {
                brokerBusy(message);
            }
        } else {
            process(Collections.singletonList(message));
        }
    }

    private boolean allowReceiverReject() {
        return config.getBoolean("receiver.reject.allow", false);
    }

    private void brokerReadOnly(ReceivingMessage message) {
        QMon.brokerReadOnlyMessageCountInc(message.getSubject());
        end(message, new ReceiveResult(message.getMessageId(), MessageProducerCode.BROKER_READ_ONLY, "BROKER BUSY", -1));
    }

    private void brokerBusy(ReceivingMessage message) {
        QMon.brokerBusyMessageCountInc(message.getSubject());
        end(message, new ReceiveResult(message.getMessageId(), MessageProducerCode.BROKER_BUSY, "BROKER BUSY", -1));
    }

    private void notAllowed(ReceivingMessage message) {
        QMon.rejectReceivedMessageCountInc(message.getSubject());
        end(message, new ReceiveResult(message.getMessageId(), MessageProducerCode.SUBJECT_NOT_ASSIGNED, "message rejected", -1));
    }

    private void error(List<ReceivingMessage> messages, Throwable e) {
        LOG.error("batch save message error", e);
        for (ReceivingMessage message : messages) {
            QMon.receivedFailedCountInc(message.getSubject());
            end(message, new ReceiveResult(message.getMessageId(), MessageProducerCode.STORE_ERROR, "store error", -1));
        }
    }

    private void offer(ReceivingMessage message, ReceiveResult result) {
        if (!message.isHigh()) {
            end(message, result);
            return;
        }
        if (!config.getBoolean("wait.slave.wrote", false)) {
            end(message, result);
            return;
        }
        if (receiveEntries.size() >= config.getInt("receive.queue.size", 50000)) {
            end(message, result);
            return;
        }
        if (result.getMessageLogOffset() < 0) {
            receiveEntries.put(receiveErrorIndex.decrementAndGet(), new ReceiveEntry(message, result));
        } else {
            receiveEntries.put(result.getMessageLogOffset(), new ReceiveEntry(message, result));
        }
    }

    private void end(ReceivingMessage message, ReceiveResult result) {
        try {
            message.done(result);
        } catch (Throwable e) {
            LOG.error("send response failed {}", message.getMessageId());
        } finally {
            QMon.processTime(message.getSubject(), System.currentTimeMillis() - message.getReceivedTime());
            message.release();
        }
    }

    private Flusher<ReceivingMessage> buildReceiverFlusher() {
        final MapConfig flushConfig = MapConfig.get("flush.properties");
        final Conf conf = Conf.fromMap(flushConfig.asMap());
        final int queueSize = conf.getInt("receiver.maxQueueSize", 1000);
        final int threads = conf.getInt("receiver.threads", 4);
        final int batchSize = conf.getInt("receiver.batchSize", 200);
        final int flushInterval = conf.getInt("receiver.flushInterval", 80);

        final Flusher<ReceivingMessage> flusher = new Flusher<>("receiver",
                batchSize,
                flushInterval,
                queueSize,
                threads,
                this);
        flushConfig.addListener(map -> flusher.setFlushInterval(Numbers.toInt(map.get("receiver.flushInterval"), flushInterval)));
        return flusher;
    }

    @Subscribe
    @SuppressWarnings("unused")
    public void syncRequest(SyncRequest syncRequest) {
        final long messageLogOffset = syncRequest.getMessageLogOffset();
        while (true) {
            final Map.Entry<Long, ReceiveEntry> entry = receiveEntries.firstEntry();
            if (entry == null) {
                break;
            }
            if (entry.getKey() > messageLogOffset) {
                break;
            }

            final ReceiveEntry value = entry.getValue();
            end(value.message, value.result);
            receiveEntries.remove(entry.getKey());
        }
    }

    private class ReceiveEntry {
        final ReceivingMessage message;
        final ReceiveResult result;

        ReceiveEntry(ReceivingMessage message, ReceiveResult result) {
            this.message = message;
            this.result = result;
        }
    }
}
