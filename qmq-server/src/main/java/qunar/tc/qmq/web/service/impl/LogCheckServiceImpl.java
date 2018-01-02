package qunar.tc.qmq.web.service.impl;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.store.MessageLogRecord;
import qunar.tc.qmq.store.MessageLogVisitor;

import java.io.IOException;
import java.util.List;

/**
 * @author yunfeng.yang
 * @since 2017/7/25
 */
public class LogCheckServiceImpl extends AbstractLogCheckService {
    private static final Logger LOG = LoggerFactory.getLogger(LogCheckServiceImpl.class);

    public void compareProduceAndLog(final String produceLogPath, final String outputPath) throws IOException {
        new Thread(new Runnable() {
            @Override
            public void run() {
                MessageLogVisitor messageLogVisitor = createMessageLogVisitor();
                List<ProduceLogEntry> producerLogs = getProduceLogEntries(produceLogPath);
                if (CollectionUtils.isEmpty(producerLogs)) {
                    LOG.error("find no produce logs");
                    return;
                }
                int index = 0;
                int size = producerLogs.size();
                Optional<MessageLogRecord> recordOptional;
                while ((recordOptional = messageLogVisitor.nextRecord()).isPresent() && index <= size) {
                    try {
                        MessageLogRecord messageLogRecord = recordOptional.get();
                        BaseMessage message;
                        if ("flight.datagrab.adapter.schedule".equals(messageLogRecord.getSubject())) {
                            message = oldDeserializeBaseMessage(messageLogRecord.getMessage());
                        } else {
                            message = deserializeBaseMessage(messageLogRecord.getMessage());
                        }
                        if (message == null) {
                            continue;
                        }
                        for (ProduceLogEntry entry : producerLogs) {
                            if (entry.getMessageId().equals(message.getMessageId()) && entry.getSubject().equals(message.getSubject())) {
                                index++;
                                entry.setFound(true);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("compare log error", e);
                    }
                }

                writeResult(outputPath, producerLogs);
                LOG.info("write result over");
            }
        }).start();
    }

    public void compareProduceAndConsume(final String produceLogPath, final String consumeLogPath, final String outputPath) throws IOException {

        new Thread(new Runnable() {
            @Override
            public void run() {
                List<ProduceLogEntry> producerLogs = getProduceLogEntries(produceLogPath);
                List<ConsumeLogEntry> consumerLogs = getConsumeLogEntries(consumeLogPath);

                if (CollectionUtils.isEmpty(producerLogs)) {
                    LOG.error("find no produce logs");
                    return;
                }

                if (CollectionUtils.isEmpty(consumerLogs)) {
                    LOG.error("find no consume logs");
                    return;
                }

                for (ConsumeLogEntry next : consumerLogs) {
                    try {
                        for (ProduceLogEntry entry : producerLogs) {
                            if (entry.getMessageId().equals(next.getMessageId()) && entry.getSubject().equals(next.getSubject())) {
                                entry.setFound(true);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("compare consume error line = {}", next.getLine(), e);
                    }
                }

                writeResult(outputPath, producerLogs);
                LOG.info("write result over");
            }
        }).start();
    }
}
