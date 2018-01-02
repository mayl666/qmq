package qunar.tc.qmq.clienttest;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.common.io.CharSink;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.consumer.MessageConsumerProvider;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class DataIntegrityTest {
    private static final Logger LOG = LoggerFactory.getLogger(DataIntegrityTest.class);

    private static final MessageProducerProvider PRODUCER = new MessageProducerProvider();
    private static final MessageConsumerProvider CONSUMER = new MessageConsumerProvider();
    private static final ThreadPoolExecutor SINGLE_EXECUTOR = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);

    private static final String SUBJECT = "qmq.test.integrity.3";
    private static final int COUNT = 10000;

    static {
        try {
            PRODUCER.afterPropertiesSet();
            CONSUMER.afterPropertiesSet();
        } catch (Exception e) {
            LOG.error("init failed.", e);
        }
    }

    public static void main(String[] args) throws Exception {
        SINGLE_EXECUTOR.execute(new ProducerTest());
        CONSUMER.addListener(SUBJECT, "g1", new ConsumeTest("g1"));

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }

    private static final class ConsumeTest implements MessageListener {
        private final AtomicInteger counter = new AtomicInteger();
        private final CharSink targetSink = Files.asCharSink(new File("/tmp/received.txt"), Charsets.UTF_8, FileWriteMode.APPEND);
        private final String group;

        ConsumeTest(String group) {
            this.group = group;
        }

        @Override
        public void onMessage(Message msg) {
            final int idx = Integer.parseInt(msg.getStringProperty("idx"));
            final String data = msg.getStringProperty("data");
            final String sha1 = msg.getStringProperty("sha1");

            if (counter.incrementAndGet() % COUNT == 0) {
                LOG.info("receive {} {}", group, idx);
            }

            if (!Objects.equals(Hashing.sha1().hashString(data, Charsets.UTF_8).toString(), sha1)) {
                LOG.error("data integrity check failed. data: {}, sha1: {}", data, sha1);
            }

            try {
                targetSink.writeLines(Collections.singletonList(data));
            } catch (IOException e) {
                LOG.error("write target file failed.", e);
            }
        }
    }

    private static final class ProducerTest implements Runnable {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public void run() {
            try {
                Files.readLines(new File("/tmp/source.txt"), Charsets.UTF_8, new LineProcessor<Void>() {
                    @Override
                    public boolean processLine(String line) throws IOException {
                        final Message message = PRODUCER.generateMessage(SUBJECT);
                        message.setProperty("idx", counter.getAndIncrement());
                        message.setProperty("data", line);
                        message.setProperty("sha1", Hashing.sha1().hashString(line, Charsets.UTF_8).toString());

                        PRODUCER.sendMessage(message);
                        if (counter.get() % COUNT == 0) {
                            LOG.info("send {}", counter.get());
                        }
                        return true;
                    }

                    @Override
                    public Void getResult() {
                        return null;
                    }
                });
            } catch (IOException e) {
                LOG.error("read source file failed.", e);
            }

            LOG.info("producer send done!!!!!!!!!!");
        }
    }
}
