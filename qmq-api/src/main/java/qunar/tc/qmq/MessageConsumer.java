/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-26
 */
public interface MessageConsumer {
    /**
     * 注册消息处理程序
     *
     * @param subjectPrefix 订阅的消息分类
     * @param consumerGroup consumer分组，用于consumer的负载均衡(broker只会给每个consumer group发送一条消息)。
     *                      如果想每个consumer进程都收到消息(广播模式)，只需要给group参数传空字符串即可。
     * @param listener      消息处理程序
     * @param executor      消息处理线程池
     * @return 返回的ListenerHolder, 表示注册关系
     */
    ListenerHolder addListener(String subjectPrefix, String consumerGroup, MessageListener listener, ThreadPoolExecutor executor);

    /**
     * 注册消息处理程序
     *
     * @param subjectPrefix 订阅的消息分类
     * @param consumerGroup consumer分组，用于consumer的负载均衡(broker只会给每个consumer group发送一条消息)。
     *                      如果想每个consumer进程都收到消息(广播模式)，只需要给group参数传空字符串即可。
     * @param listener      消息处理程序
     * @param executor      消息处理线程池
     * @param rejectPolicy  处理队列满时的拒绝策略(仅仅提供给非可靠消息队列使用)
     * @return 返回的ListenerHolder, 表示注册关系
     */
    @Deprecated
    ListenerHolder addListener(String subjectPrefix, String consumerGroup, MessageListener listener, ThreadPoolExecutor executor, RejectPolicy rejectPolicy);

}
