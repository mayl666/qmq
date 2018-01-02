/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.producer.tx;

import org.springframework.jdbc.core.JdbcTemplate;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageStore;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.SqlConstant;
import qunar.tc.qmq.producer.sender.PositionSpy;
import qunar.tc.qmq.serializer.SerializerFactory;

import javax.sql.DataSource;
import java.sql.Timestamp;

import static qunar.tc.qmq.MessageState.BROKER_BUSY;
import static qunar.tc.qmq.MessageState.MESSAGE_BLOCKED;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-6 多数据源的store
 * <p/>
 * 与业务的RoutingDataSource数据源隔离，避免业务数据源扭转时干扰qmq
 */
class MultiDataSourceMessageStore implements MessageStore {
    private final JdbcTemplate product;

    private final JdbcTemplate platform;

    private final InstanceSingletonDataSource platformDataSource;

    MultiDataSourceMessageStore(DataSource proDs, InstanceSingletonDataSource platformDataSource) {
        this.product = new JdbcTemplate(proDs);
        this.platform = new JdbcTemplate(platformDataSource);
        this.platformDataSource = platformDataSource;
    }

    @Override
    public void insertNew(ProduceMessage message) {
        if (message.getBase().isStoreAtFailed()) return;

        // 插入新消息时将当前业务数据源url记录到message，当finish和error时从message里取出url，扭转数据源
        String url = JdbcUtils.getDatabaseInstanceURL(product.getDataSource());
        product.update(SqlConstant.insertSQL, message.getMessageId(), new Timestamp(System.currentTimeMillis()), SerializerFactory.create().serialize(message.getBase()));
        message.setStoreKey(url);
    }

    @Override
    public void finish(ProduceMessage message) {
        if (message.getBase().isStoreAtFailed()) return;

        platformDataSource.setDataBaseURL((String) message.getStoreKey());
        platform.update(SqlConstant.finishSQL, message.getMessageId());
    }

    @Override
    public void error(ProduceMessage message) {
        Message base = message.getBase();
        if (base.isStoreAtFailed()) {
            product.update(SqlConstant.insertSQL, message.getMessageId(), new Timestamp(System.currentTimeMillis()), SerializerFactory.create().serialize(base));
        } else {
            platformDataSource.setDataBaseURL((String) message.getStoreKey());
            platform.update(SqlConstant.errorSQL, BROKER_BUSY.getCode(), new Timestamp(System.currentTimeMillis()), message.getMessageId());
        }
    }

    @Override
    public void block(ProduceMessage message) {
        Message base = message.getBase();
        if (base.isStoreAtFailed()) {
            product.update(SqlConstant.insertSQL, message.getMessageId(), new Timestamp(System.currentTimeMillis()), SerializerFactory.create().serialize(base));
        } else {
            platformDataSource.setDataBaseURL((String) message.getStoreKey());
            platform.update(SqlConstant.errorSQL, MESSAGE_BLOCKED.getCode(), new Timestamp(System.currentTimeMillis()), message.getMessageId());
        }
    }

    @Override
    public void report() {
        PositionSpy.notify(platformDataSource.keys());
    }

    @Override
    public void beginTransaction() {
    }

    @Override
    public String dsIndex(boolean inTransaction) {
        return null;
    }

    @Override
    public void endTransaction() {

    }
}
