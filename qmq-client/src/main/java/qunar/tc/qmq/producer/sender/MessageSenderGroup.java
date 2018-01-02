package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.netty.exception.RemoteBusyException;
import qunar.tc.qmq.netty.exception.RemoteTimeoutException;
import qunar.tc.qmq.producer.SendErrorHandler;
import qunar.tc.qmq.service.exceptions.BlockMessageException;
import qunar.tc.qmq.service.exceptions.DuplicateMessageException;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author zhenyu.nie created on 2017 2017/7/5 17:26
 */
class MessageSenderGroup {

    private final Connection connection;
    private final SendErrorHandler errorHandler;

    private final List<ProduceMessage> source;

    MessageSenderGroup(SendErrorHandler errorHandler, Connection connection) {
        this.errorHandler = errorHandler;
        this.connection = connection;
        this.source = new ArrayList<>();
    }

    public void send() {
        Map<String, MessageException> map;
        try {
            map = connection.send(source);
        } catch (RemoteTimeoutException | RemoteBusyException e) {
            //如果是超时和忙的话就不立即重试了，服务器端是异步的，一般情况下不会因为线程等原因引起
            //超时，基本上是因为数据库flush慢导致的，这样重试只会加重问题
            for (ProduceMessage pm : source) {
                errorHandler.failed(pm, e);
            }
            return;
        } catch (Exception e) {
            for (ProduceMessage pm : source) {
                errorHandler.error(pm, e);
            }
            //fuck this return
            return;
        }

        if (map == null) {
            for (ProduceMessage pm : source) {
                errorHandler.error(pm, new MessageException(pm.getMessageId(), "return null"));
            }
            return;
        }

        if (map.isEmpty())
            map = Collections.emptyMap();

        for (ProduceMessage pm : source) {
            MessageException ex = map.get(pm.getMessageId());
            if (ex == null || ex instanceof DuplicateMessageException) {
                errorHandler.finish(pm, ex);
            } else {
                //如果是消息被拒绝，说明broker已经限速，不立即重试;
                if (ex.isBrokerBusy()) {
                    errorHandler.failed(pm, ex);
                } else if (ex instanceof BlockMessageException) {
                    //如果是block的,证明还没有被授权,也不重试,task也不重试,需要手工恢复
                    errorHandler.block(pm, ex);
                } else {
                    errorHandler.error(pm, ex);
                }
            }
        }
    }

    void addMessage(ProduceMessage source) {
        this.source.add(source);
    }
}
