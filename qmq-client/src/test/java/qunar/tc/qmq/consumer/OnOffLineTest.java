package qunar.tc.qmq.consumer;

import qunar.tc.qmq.consumer.register.QConfigConsumerRegister;

/**
 * @author yiqun.fan create on 17-9-14.
 */
public class OnOffLineTest {

    public void test(MessageConsumerProvider provider) {
        QConfigConsumerRegister register = provider.getRegister();
        for (int i = 3; i > 0; i--) {
            register.online();
            register.offline();
        }
        register.online();
    }
}
