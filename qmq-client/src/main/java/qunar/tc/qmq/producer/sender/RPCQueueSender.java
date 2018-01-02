/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.producer.sender;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.batch.BatchExecutor;
import qunar.tc.qmq.batch.Processor;
import qunar.tc.qmq.common.QmqLogger;
import qunar.tc.qmq.netty.exception.SubjectNotAssignedException;
import qunar.tc.qmq.producer.QueueSender;
import qunar.tc.qmq.producer.SendErrorHandler;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-6
 */
class RPCQueueSender implements QueueSender, SendErrorHandler, Processor<ProduceMessage> {

    private static final Logger log = LoggerFactory.getLogger(RPCQueueSender.class);

    private final BatchExecutor<ProduceMessage> executor;

    private final RouterManager routerManager;

    public RPCQueueSender(String name, int maxQueueSize, int sendThreads, int sendBatch, RouterManager routerManager) {
        this.routerManager = routerManager;

        this.executor = new BatchExecutor<ProduceMessage>(name, sendBatch, this);
        this.executor.setQueueSize(maxQueueSize);
        this.executor.setThreads(sendThreads);
        this.executor.init();
    }

    @Override
    public boolean offer(ProduceMessage pm) {
        return this.executor.addItem(pm);
    }

    @Override
    public boolean offer(ProduceMessage pm, long millisecondWait) {
        boolean inserted;
        try {
            inserted = this.executor.addItem(pm, millisecondWait, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
        return inserted;
    }

    @Override
    public void send(ProduceMessage pm) {
        process(Arrays.asList(pm));
    }

    @Override
    public void destroy() {
        executor.destroy();
    }

    @Override
    public void process(List<ProduceMessage> list) {
        //按照路由分组发送
        Collection<MessageSenderGroup> messages = groupBy(list);
        for (MessageSenderGroup group : messages) {
            group.send();
        }
    }

    private Collection<MessageSenderGroup> groupBy(List<ProduceMessage> list) {
        Map<Connection, MessageSenderGroup> map = Maps.newHashMap();
        for (int i = 0; i < list.size(); ++i) {
            ProduceMessage produceMessage = list.get(i);
            Connection connection = routerManager.routeOf(produceMessage.getBase());
            MessageSenderGroup group = map.get(connection);
            if (group == null) {
                group = new MessageSenderGroup(this, connection);
                map.put(connection, group);
            }
            group.addMessage(produceMessage);
        }
        return map.values();
    }

    @Override
    public void error(ProduceMessage pm, Exception e) {
        if (!(e instanceof SubjectNotAssignedException)) {
            log.warn("Message 发送失败! {}", pm.getMessageId(), e);
        }
        pm.error(e);
    }

    @Override
    public void failed(ProduceMessage pm, Exception e) {
        log.warn("Message 发送失败! {}", pm.getMessageId(), e);
        pm.failed();
    }

    @Override
    public void block(ProduceMessage pm, MessageException ex) {
        log.warn("Message 发送失败! {},被server拒绝,请检查应用授权配置,如果需要恢复消息请手工到db恢复状态", pm.getMessageId(), ex);
        pm.block();
    }

    @Override
    public void finish(ProduceMessage pm, Exception e) {
        log.debug("Message 成功 {}", pm.getMessageId(), e);
        QmqLogger.log(pm.getBase(), "发送成功." + (e == null ? "" : "消息重复"));
        pm.finish();
    }
}
