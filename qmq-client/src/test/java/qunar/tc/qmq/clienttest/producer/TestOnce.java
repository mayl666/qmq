package qunar.tc.qmq.clienttest.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhenyu.nie created on 2017 2017/7/11 15:52
 */
public class TestOnce {

    private static final Logger logger = LoggerFactory.getLogger(TestOnce.class);

    private static AtomicInteger i = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        final MessageProducerProvider provider = new MessageProducerProvider();
        provider.afterPropertiesSet();
        while (true) {
            Message message = provider.generateMessage("testnewqmq");
            message.setProperty("the data", "haha" + i.get());
            logger.info("send message: {}", message);
            System.in.read();
            provider.sendMessage(message);
        }
    }
}
