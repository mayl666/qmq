package qunar.tc.qmq.clienttest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yiqun.fan create on 17-7-7.
 */
public class PullTest {
    private static final Logger LOG = LoggerFactory.getLogger(PullTest.class);

    private static final Random random = new Random();
    private static final ConcurrentMap<String, AtomicInteger> messageCount = new ConcurrentHashMap<>();

    public static void main(final String[] args) throws Exception {
        MessageConsumerProvider consumer = new MessageConsumerProvider();
        consumer.afterPropertiesSet();
        final CountDownLatch latch = new CountDownLatch(2);
        final String prefix = "qmq.test.19";
        final AtomicLong counter = new AtomicLong(0);
        consumer.addListener(prefix, "qmqtest", new MessageListener() {
            final AtomicInteger count = new AtomicInteger(0);

            @Override
            public void onMessage(Message msg) {
//                throw new RuntimeException("test error");
//                try {
//                    TimeUnit.MILLISECONDS.sleep(500);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                throw new RuntimeException("err test");
//                if (random.nextInt(10) == 7) {
//                    counter2.incrementAndGet();
//                    throw new RuntimeException("err test");
//                }
                counter.incrementAndGet();
//                AtomicInteger old = messageCount.putIfAbsent(msg.getMessageId(), new AtomicInteger(1));
//                if (old != null) {
//                    int count = old.incrementAndGet();
//                    LOG.info("duplicate msg count {}, msg {}", count, msg.getMessageId(), msg.times());
//                }
//                System.out.println("group1");
//                print(count.getAndIncrement(), msg);
            }
        });
//        consumer.addListener(prefix, "group2", new MessageListener() {
//            final AtomicInteger count = new AtomicInteger(0);
//
//            @Override
//            public void onMessage(Message msg) {
//                if (count.incrementAndGet() % 5 == 0) {
//                    throw new RuntimeException("err test 2");
//                }
//                System.out.println("group2");
////                print(count.getAndIncrement(), msg);
//            }
//        });

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    LOG.info("COUNTER: {}", counter);
                }
            }
        }).start();
        latch.await();
        consumer.destroy();
    }

    private static void print(int count, Message message) {
        System.out.println(">> " + message.getSubject() + " " + count + "  " + message.getMessageId() + "  " + message.getAttrs());
    }
}
