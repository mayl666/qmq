package qunar.tc.qmq.clienttest;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.ReliabilityLevel;
import qunar.tc.qmq.consumer.MessageConsumerProvider;
import qunar.tc.qmq.consumer.OnOffLineTest;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yiqun.fan create on 17-8-20.
 */
public class ClientTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientTest.class);
    private static final MessageProducerProvider producerProvider = new MessageProducerProvider();
    private static final MessageConsumerProvider consumerProvider = new MessageConsumerProvider();

    static {
        try {
            producerProvider.afterPropertiesSet();
            consumerProvider.afterPropertiesSet();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final String subject = "qmq.test.d.1";
    private static final boolean enableCheck = true;
    private static final int SEND_QPS = 10000;
    private static final int SEND_NUM = 20_0000;

    private static final Executor executor = Executors.newFixedThreadPool(10);

    public static void main(String[] args) throws Exception {
        OnOffLineTest onOffLineTest = new OnOffLineTest();
        onOffLineTest.test(consumerProvider);

        executor.execute(new ProducerTest());
        Thread.sleep(5000);

        consumerProvider.addListener(subject, "g1", new ConsumeTest("g1"));
        consumerProvider.addListener(subject, "g2", new ConsumeTest("g2"));

        new CountDownLatch(1).await();
    }

    private static final class ConsumeTest implements MessageListener {
        private final String group;
        private final RateLimiter logLimiter = RateLimiter.create(1);
        private final Qps qps = new Qps();
        private final int[] result = new int[SEND_NUM];
        private final AtomicInteger count = new AtomicInteger(0);

        ConsumeTest(String group) {
            this.group = group;
        }

        @Override
        public void onMessage(Message msg) {
            count.getAndIncrement();
            qps.inc();
            if (logLimiter.tryAcquire()) {
                LOGGER.info("receive-{} {} {}", group, qps.qps(), count.get());
            }
            if (enableCheck) {
                int data = Integer.parseInt(msg.getStringProperty("data"));
                if (data < 0 || data > SEND_NUM) {
                    LOGGER.error("receive-{} data: {}", data);
                }
                if (result[data] != 0) {
                    LOGGER.error("receive-{} duplicated data: {}", group, data);
                }
                result[data] = 1;
            }
            if (count.get() >= SEND_NUM) {
                if (enableCheck) {
                    for (int i = 0; i < SEND_NUM; i++) {
                        if (result[i] != 1) {
                            LOGGER.error("receive-{} error: {}", group, i);
                        }
                    }
                }
                LOGGER.info("receive-{} num: {}", group, count.get());
            }
        }
    }

    private static final class ProducerTest implements Runnable {
        private final RateLimiter limiter = RateLimiter.create(SEND_QPS);
        private final AtomicInteger count = new AtomicInteger(0);
        private final RateLimiter logLimiter = RateLimiter.create(1);
        private final Qps qps = new Qps();

        @Override
        public void run() {
            while (true) {
                limiter.acquire();
                Message message = producerProvider.generateMessage(subject);
                message.setProperty("data", String.valueOf(count.getAndIncrement()));
                message.setReliabilityLevel(ReliabilityLevel.High);
                producerProvider.sendMessage(message, new MessageSendStateListener() {
                    @Override
                    public void onSuccess(Message message) {
                    }

                    @Override
                    public void onFailed(Message message) {
                    }
                });
                qps.inc();
                if (logLimiter.tryAcquire()) {
                    LOGGER.info("send {}", qps.qps());
                }
                if (count.get() >= SEND_NUM) {
                    break;
                }
            }
        }
    }

    private static final class Qps {
        private final AtomicInteger count = new AtomicInteger(0);
        private final AtomicLong time = new AtomicLong();

        void inc() {
            count.getAndIncrement();
        }

        long qps() {
            long last = time.get();
            int num = count.get();
            count.getAndAdd(-num);
            long curr = System.currentTimeMillis();
            time.set(curr);
            if (last >= curr) {
                return num;
            }
            return num * 1000 / (curr - last);
        }
    }
}
