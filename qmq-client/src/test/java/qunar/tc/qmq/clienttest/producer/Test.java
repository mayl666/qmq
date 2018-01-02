package qunar.tc.qmq.clienttest.producer;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhenyu.nie created on 2017 2017/7/7 16:41
 */
public class Test {
    private static final Logger LOG = LoggerFactory.getLogger(Test.class);

    private static final AtomicInteger COUNTER = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        final RateLimiter limiter = RateLimiter.create(3000);
        final MessageProducerProvider provider = new MessageProducerProvider();
        provider.afterPropertiesSet();

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        limiter.acquire();
                        if (COUNTER.get() == 50000) {
                            break;
                        }
                        final Message message = provider.generateMessage("qmq.test.19");
                        message.setProperty("mydata", "haha:" + COUNTER.incrementAndGet());
                        provider.sendMessage(message, new MessageSendStateListener() {
                            @Override
                            public void onSuccess(Message message) {

                            }

                            @Override
                            public void onFailed(Message message) {

                            }
                        });
//                        System.out.println(message);

//                        System.in.read();
                    } catch (Exception e) {
                        LOG.error("", e);
                    }
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    LOG.info("COUNTER: {}", COUNTER);
                }
            }
        }).start();


        TimeUnit.DAYS.sleep(100);
    }
}
