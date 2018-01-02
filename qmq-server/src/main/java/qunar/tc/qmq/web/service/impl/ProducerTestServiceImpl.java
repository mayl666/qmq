package qunar.tc.qmq.web.service.impl;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.api.pojo.node.JacksonSupport;
import qunar.metrics.Counter;
import qunar.metrics.Metrics;
import qunar.metrics.Timer;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.ReliabilityLevel;
import qunar.tc.qmq.producer.MessageProducerProvider;
import qunar.tc.qmq.serializer.Serializer;
import qunar.tc.qmq.serializer.SerializerFactory;
import qunar.tc.qmq.web.service.ProducerTestService;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author yunfeng.yang
 * @since 2017/7/11
 */
public class ProducerTestServiceImpl implements ProducerTestService {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerTestServiceImpl.class);

    private static final String PROPERTY_DATA = "data";

    private static final Timer SEND_TIME = Metrics.timer("ProducerTest.SendTime").get();
    private static final Counter SEND_SUCCESS = Metrics.counter("ProducerTest.SendSuccess").delta().get();
    private static final Counter SEND_FAIL = Metrics.counter("ProducerTest.SendFail").delta().get();

    private final Serializer serializer = SerializerFactory.create();
    private final MessageProducerProvider messageProducer;
    private final AtomicBoolean sendSwitch = new AtomicBoolean(false);

    public ProducerTestServiceImpl() {
        messageProducer = new MessageProducerProvider();
        try {
            messageProducer.afterPropertiesSet();
        } catch (Exception e) {
            LOG.error("provider init error", e);
        }
    }

    @Override
    public void start(final double qps, final String subject, final int len, final boolean isLow) {

        final RateLimiter rateLimiter = RateLimiter.create(qps);
        sendSwitch.compareAndSet(false, true);
        new Thread(new Runnable() {
            @Override
            public void run() {
                int i = 0;
                while (sendSwitch.get()) {
                    final Timer.Context time = SEND_TIME.time();

                    rateLimiter.acquire();
                    try {
                        final String data = generateData(len);
                        final HashFunction hash = Hashing.sha1();
                        final HashCode code = hash.hashString(data, Charsets.UTF_8);

                        final Message message = generateMessage(subject, data);
                        message.setProperty("code", code.asInt());
                        message.setProperty("result", Boolean.TRUE);
                        message.setProperty("today", new Date());
                        if (isLow) {
                            message.setReliabilityLevel(ReliabilityLevel.Low);
                        }
                        messageProducer.sendMessage(message, new MessageSendStateListener() {
                            @Override
                            public void onSuccess(Message message) {
                                SEND_SUCCESS.inc();
                                time.stop();
                            }

                            @Override
                            public void onFailed(Message message) {
                                LOG.info("fail, msg{}", JacksonSupport.toJson(message));
                                SEND_FAIL.inc();
                                time.stop();
                            }
                        });
                    } catch (Exception e) {
                        LOG.error("send error", e);
                    }

                    i++;
                    if (i % 5000 == 0) {
                        LOG.info("send: {}", i);
                    }
                }
            }
        }).start();
    }

    public static void main(String[] args) {
        String s = generateData(1024);
        System.out.println(s);
    }

    public static String generateData(int len) {
        Random random = new Random(System.currentTimeMillis());
        StringBuilder result = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            result.append((char) ('a' + random.nextInt(26)));
        }
        return result.toString();
    }

    private Message generateMessage(String subject, Object data) {
        Message message = messageProducer.generateMessage(subject);
        setMessageData(message, PROPERTY_DATA, data);
        return message;
    }

    private void setMessageData(Message message, String name, Object value) {
        if (value == null)
            return;
        if (value instanceof String) {
            message.setProperty(name, (String) value);
        } else if (value instanceof Boolean) {
            message.setProperty(name, (Boolean) value);
        } else if (value instanceof Integer) {
            message.setProperty(name, (Integer) value);
        } else if (value instanceof Long) {
            message.setProperty(name, (Long) value);
        } else if (value instanceof Float) {
            message.setProperty(name, (Float) value);
        } else if (value instanceof Double) {
            message.setProperty(name, (Double) value);
        } else if (value instanceof Date) {
            message.setProperty(name, (Date) value);
        } else {
            message.setProperty(name, serializer.serialize(value));
        }
    }

    @Override
    public void stop() {
        sendSwitch.set(false);
    }
}
