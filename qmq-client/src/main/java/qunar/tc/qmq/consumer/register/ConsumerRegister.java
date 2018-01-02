package qunar.tc.qmq.consumer.register;

import qunar.management.Switchable;
import qunar.tc.qmq.MessageListener;

/**
 * User: zhaohuiyu
 * Date: 6/5/13
 * Time: 10:59 AM
 */
public interface ConsumerRegister extends Switchable {

    void regist(String prefix, String group, RegistParam param);

    void unregist(String prefix, String group);

    String registry();

    void setAutoOnline(boolean autoOnline);

    void destroy();
}
