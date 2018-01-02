package qunar.tc.qmq.web.service.impl;

import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.configuration.QConfig;
import qunar.tc.qmq.store.MessageLog;
import qunar.tc.qmq.store.MessageLogVisitor;
import qunar.tc.qmq.store.MessageStoreConfig;
import qunar.tc.qmq.store.MessageStoreConfigImpl;
import qunar.tc.qmq.utils.CharsetUtils;
import qunar.tc.qmq.web.service.LogCheckService;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author yunfeng.yang
 * @since 2017/7/25
 */
abstract class AbstractLogCheckService implements LogCheckService {
    private static final Logger logger = LoggerFactory.getLogger(AbstractLogCheckService.class);

    private static final Pattern PRODUCE_PATTERN = Pattern.compile("\\[(.*):(.*)]");
    private static final String CONSUME_STRING = "收到一条消息,此消息会被匹配到:";
    private static final String PRODUCE_STRING = "发送成功.";

    private static final Splitter SPLITTER = Splitter.on(CharMatcher.BREAKING_WHITESPACE).omitEmptyStrings().trimResults();


    private static final String NOT_FOUND_PRODUCE_LOG = "not_found_produce";
    private static final String FOUND_PRODUCE_LOG = "found_produce";
    private final MessageStoreConfig config = new MessageStoreConfigImpl(new QConfig());

    List<ProduceLogEntry> getProduceLogEntries(String produceLogPath) {
        File file = new File(produceLogPath);
        try {
            return Files.readLines(file, Charsets.UTF_8, new LineProcessor<List<ProduceLogEntry>>() {
                final List<ProduceLogEntry> result = Lists.newArrayList();

                @Override
                public boolean processLine(String line) throws IOException {
                    List<String> strings = SPLITTER.splitToList(line);
                    if (strings.size() != 4 && !PRODUCE_STRING.equals(strings.get(3))) {
                        return true;
                    }
                    Matcher produceMatcher = PRODUCE_PATTERN.matcher(strings.get(2));
                    if (!produceMatcher.matches()) {
                        return true;
                    }
                    ProduceLogEntry entry = new ProduceLogEntry(line, produceMatcher.group(1), produceMatcher.group(2));
                    result.add(entry);
                    return true;
                }

                @Override
                public List<ProduceLogEntry> getResult() {
                    return result;
                }
            });
        } catch (IOException e) {
            logger.error("read logs error", e);
        }
        return null;
    }

    List<ConsumeLogEntry> getConsumeLogEntries(String consumeLogPath) {
        File consumerFile = new File(consumeLogPath);
        try {
            return Files.readLines(consumerFile, Charsets.UTF_8, new LineProcessor<List<ConsumeLogEntry>>() {
                final List<ConsumeLogEntry> result = Lists.newArrayList();

                @Override
                public boolean processLine(String line) throws IOException {
                    List<String> strings = SPLITTER.splitToList(line);
                    if (strings.size() != 5 && !CONSUME_STRING.equals(strings.get(3))) {
                        return true;
                    }
                    Matcher consumeMatcher = PRODUCE_PATTERN.matcher(strings.get(2));
                    if (!consumeMatcher.matches()) {
                        return true;
                    }
                    ConsumeLogEntry entry = new ConsumeLogEntry(line,
                            consumeMatcher.group(1), consumeMatcher.group(2));
                    result.add(entry);
                    return true;
                }

                @Override
                public List<ConsumeLogEntry> getResult() {
                    return result;
                }
            });
        } catch (IOException e) {
            logger.error("find consume logs error", e);
        }
        return null;
    }


    private void writeLines(List<String> lines, File to) {
        for (String line : lines) {
            try {
                Files.append(line + System.lineSeparator(), to, Charsets.UTF_8);
            } catch (IOException e) {
                logger.error("write line error line: {}", line, e);
            }
        }
    }

    private File createFile(final String filePath, final String fileName) {
        File file = new File(filePath + fileName);
        if (file.exists() && !file.delete()) {
            throw new RuntimeException(fileName + " still exists and failed to delete file ");
        }
        return file;
    }

    BaseMessage oldDeserializeBaseMessage(ByteBuffer body) {
        try {
            int flag = body.getInt();
            long createdTime = body.getLong();
            long expiredTime = body.getLong();

            byte[] bytes;

            int subjectLen = body.getInt();
            bytes = new byte[subjectLen];
            body.get(bytes);
            String subject = CharsetUtils.toUTF8String(bytes);

            int messageIdLen = body.getInt();
            bytes = new byte[messageIdLen];
            body.get(bytes);
            String messageId = CharsetUtils.toUTF8String(bytes);

            int attachLen = body.getInt();
            bytes = new byte[attachLen];
            body.get(bytes);
//        Map<String, String> headerProperties = deserializeMapString(bytes);

            int bodyLen = body.getInt();
            bytes = new byte[bodyLen];
            body.get(bytes);
//        HashMap<String, Object> attrs = deserializeMap(bytes);

            BaseMessage message = new BaseMessage();
            message.setMessageId(messageId);
            message.setSubject(subject);

            return message;
        } catch (Exception e) {
            // ignore
            return null;
        }
    }

    BaseMessage deserializeBaseMessage(ByteBuffer body) {
        try {
            byte[] flagByte = new byte[1];
            body.get(flagByte);
            long createdTime = body.getLong();
            long expiredTime = body.getLong();

            int subjectLen = body.getShort();
            byte[] subjectBs = new byte[subjectLen];
            body.get(subjectBs);
            String subject = CharsetUtils.toUTF8String(subjectBs);

            int messageIdLen = body.getShort();
            byte[] messageIdBs = new byte[messageIdLen];
            body.get(messageIdBs);
            String messageId = CharsetUtils.toUTF8String(messageIdBs);

            int attachLen = body.getInt();
            byte[] attachBs = new byte[attachLen];
            body.get(attachBs);

            int bodyLen = body.getInt();
            byte[] bodyBs = new byte[bodyLen];
            body.get(bodyBs);

            BaseMessage message = new BaseMessage();
            message.setMessageId(messageId);
            message.setSubject(subject);

            return message;
        } catch (Exception e) {
            // ignore
            return null;
        }
    }


    void writeResult(String outputPath, List<ProduceLogEntry> producerLogs) {
        List<String> produceFound = Lists.newArrayList();
        List<String> produceNotFound = Lists.newArrayList();
        for (ProduceLogEntry entry : producerLogs) {
            if (entry.isFound()) {
                produceFound.add(entry.getLine());
            } else {
                produceNotFound.add(entry.getLine());
            }
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
        File to = createFile(outputPath, NOT_FOUND_PRODUCE_LOG + simpleDateFormat.format(new Date(System.currentTimeMillis())) + ".log");
        writeLines(produceNotFound, to);

        File foundFile = createFile(outputPath, FOUND_PRODUCE_LOG + simpleDateFormat.format(new Date(System.currentTimeMillis())) + ".log");
        writeLines(produceFound, foundFile);
    }


    MessageLogVisitor createMessageLogVisitor() {
        final MessageLog messageLog = new MessageLog(config, null);
        return messageLog.newLogVisitor();
    }


    static class ConsumeLogEntry {
        final String line;
        final String messageId;
        final String subject;

        ConsumeLogEntry(String line, String messageId, String subject) {
            this.line = line;
            this.messageId = messageId;
            this.subject = subject;
        }

        String getLine() {
            return line;
        }

        String getMessageId() {
            return messageId;
        }

        String getSubject() {
            return subject;
        }
    }

    static class ProduceLogEntry {
        final String line;
        final String messageId;
        final String subject;
        boolean found;

        ProduceLogEntry(String line, String messageId, String subject) {
            this.line = line;
            this.messageId = messageId;
            this.subject = subject;
        }

        String getLine() {
            return line;
        }

        String getMessageId() {
            return messageId;
        }

        String getSubject() {
            return subject;
        }

        boolean isFound() {
            return found;
        }

        void setFound(boolean found) {
            this.found = found;
        }
    }
}
