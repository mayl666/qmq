/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq;

import java.util.concurrent.TimeUnit;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-26
 */
public interface MessageProducer {

    /**
     * 在发送消息之前调用该接口生成消息，该接口会生成唯一消息id。这条消息的过期时间为默认的15分钟
     *
     * @param subject 要发送的消息的subject
     * @return 生成的消息
     */
    Message generateMessage(String subject);

    /**
     * 生成一条带有过期时间的消息。过期的时间为当前时间后的多长时间
     *
     * @param subject  消息的subject
     * @param expire   多长时间后过期 过期时间不能超过24小时
     * @param timeUnit 时间的单位
     * @return
     */
    Message generateMessage(String subject, long expire, TimeUnit timeUnit);

    /**
     * 生成消息，用户提供消息ID。默认过期时间15分钟
     *
     * @param messageId 用户提供的消息ID
     * @param subject   消息subject
     * @return 生成的消息
     */
    Message generateMessage(String messageId, String subject);

    /**
     * 生成消息，用户提供消息ID，设置过期时间
     *
     * @param messageId 用户提供的消息ID
     * @param subject   消息subject
     * @param expire    多长时间后过期 过期时间不能超过24小时
     * @param timeUnit  时间的单位
     * @return
     */
    Message generateMessage(String messageId, String subject, long expire, TimeUnit timeUnit);

    /**
     * 发送消息
     * 注意：在使用事务性消息时该方法仅将消息入库，当事务成功提交时才发送消息。只要事务提交，消息就会发送(即使入库失败)。
     * 在使用事务消息时，该方法会使用业务方配置的数据源操作qmq_produce数据库，请确保为业务datasource配置的数据库用户
     * 具有对qmq_produce数据库的CURD权限。
     * <p/>
     * 该方法须在generateMessage方法调用之后调用 @see generateMessage
     *
     * @param message
     * @throws RuntimeException 当消息体超过指定大小(60K)的时候会抛出RuntimeException异常
     *                          当消息过期时间设置非法的时候会抛出RuntimeException异常
     */
    void sendMessage(Message message);

    void sendMessage(Message message, MessageSendStateListener listener);
}
