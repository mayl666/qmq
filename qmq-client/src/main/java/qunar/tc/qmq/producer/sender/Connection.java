package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.netty.exception.BrokerRejectException;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.netty.exception.RemoteException;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.List;
import java.util.Map;

/**
 * @author zhenyu.nie created on 2017 2017/7/3 12:25
 */
public interface Connection {

    void preHeat();

    String url();

    Map<String, MessageException> send(List<ProduceMessage> messages) throws RemoteException, ClientSendException, BrokerRejectException;

    void destroy();
}
