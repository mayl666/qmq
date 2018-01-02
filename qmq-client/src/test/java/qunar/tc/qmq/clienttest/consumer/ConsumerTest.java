package qunar.tc.qmq.clienttest.consumer;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.consumer.MessageConsumerProvider;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yiqun.fan create on 17-8-20.
 */
public class ConsumerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerTest.class);

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

    private static final String subject = "qmq.test.19";
    private static final int SEND_QPS = 100000;

    private static final Executor executor = Executors.newFixedThreadPool(10);

    public static void main(String[] args) throws Exception {
        executor.execute(new ProducerTest());

        consumerProvider.addListener(subject, "g1", new ConsumeTest("g1"));
//        consumerProvider.addListener(subject, "g2", new ConsumeTest("g2"));

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }

    private static final class ConsumeTest implements MessageListener {
        private final String group;
        private final AtomicInteger count = new AtomicInteger(0);

        ConsumeTest(String group) {
            this.group = group;
        }

        @Override
        public void onMessage(Message msg) {
//            int data = Integer.parseInt(msg.getStringProperty("data"));
            if (count.getAndIncrement() % SEND_QPS == 0) {
//                LOGGER.info("receive {} {}", group, data);
//                throw new RuntimeException("test");
                LOGGER.info("receive count: {}", count);
            }
        }
    }

    private static final class ProducerTest implements Runnable {
        private final AtomicInteger count = new AtomicInteger(0);
        private final RateLimiter limiter = RateLimiter.create(SEND_QPS);

        @Override
        public void run() {
            while (true) {
                limiter.acquire();
                Message message = producerProvider.generateMessage(subject);
                message.setProperty("data", String.valueOf(count.getAndIncrement()));
                producerProvider.sendMessage(message, new MessageSendStateListener() {
                    @Override
                    public void onSuccess(Message message) {
                    }

                    @Override
                    public void onFailed(Message message) {
                    }
                });
                if (count.get() % SEND_QPS == 0) {
                    LOGGER.info("send {}", count.get());
                }
                if (count.get() > 5000_0000) {
                    break;
                }
            }
        }
    }
}
