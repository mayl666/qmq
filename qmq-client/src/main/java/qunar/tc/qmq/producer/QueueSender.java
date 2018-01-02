/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.producer;


import qunar.tc.qmq.ProduceMessage;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-6
 */
public interface QueueSender {

    boolean offer(ProduceMessage pm);

    boolean offer(ProduceMessage pm, long millisecondWait);

    void send(ProduceMessage pm);

    void destroy();
}
