/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-5
 */
public interface MessageStore {

    /**
     * 将消息持久化到本地存储
     * @param message
     */
    void insertNew(ProduceMessage message);

    /**
     * 发送出错
     * @param message
     */
    void error(ProduceMessage message);

    /**
     * 发送完成，删除消息
     * @param message
     */
    void finish(ProduceMessage message);

    /**
     * server设置了白名单
     * @param message
     */
    void block(ProduceMessage message);

    /**
     * 将本地存储汇报
     */
    void report();

    /**
     * 事务开始
     */
    void beginTransaction();

    /**
     * 事务结束
     */
    void endTransaction();

    /**
     *
     * @param inTransaction
     * @return
     */
    String dsIndex(boolean inTransaction);

}

