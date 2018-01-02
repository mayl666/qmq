/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.producer.tx;

import org.springframework.jdbc.core.JdbcTemplate;
import qunar.tc.qmq.MessageStore;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.SqlConstant;
import qunar.tc.qmq.producer.wrapper.DataSourceWrapper;
import qunar.tc.qmq.producer.wrapper.WrapperFactory;
import qunar.tc.qmq.serializer.SerializerFactory;

import javax.sql.DataSource;
import java.sql.Timestamp;

import static qunar.tc.qmq.MessageState.BROKER_BUSY;
import static qunar.tc.qmq.MessageState.MESSAGE_BLOCKED;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-9
 */
class SingleDataSourceMessageStore implements MessageStore {

    private final DataSource dataSource;

    private final JdbcTemplate platform;

    private DataSourceWrapper wrapper;

    SingleDataSourceMessageStore(DataSource datasource) {
        this.dataSource = datasource;
        this.platform = new JdbcTemplate(datasource);
    }

    @Override
    public void insertNew(ProduceMessage message) {
        if (message.getBase().isStoreAtFailed()) return;

        Object source = wrapper.route(message);
        try {
            platform.update(SqlConstant.insertSQL, message.getMessageId(), new Timestamp(System.currentTimeMillis()), SerializerFactory.create().serialize(message.getBase()));
        } finally {
            wrapper.routeAfter(source);
        }
    }

    @Override
    public void finish(ProduceMessage message) {
        if (message.getBase().isStoreAtFailed()) return;

        Object source = wrapper.route(message);
        try {
            platform.update(SqlConstant.finishSQL, message.getMessageId());
        } finally {
            wrapper.routeAfter(source);
        }
    }

    @Override
    public void error(ProduceMessage message) {
        Object source = wrapper.route(message);
        try {
            if (message.getBase().isStoreAtFailed()) {
                platform.update(SqlConstant.insertSQL, message.getMessageId(), new Timestamp(System.currentTimeMillis()), SerializerFactory.create().serialize(message.getBase()));
            } else {
                platform.update(SqlConstant.errorSQL, BROKER_BUSY.getCode(), new Timestamp(System.currentTimeMillis()), message.getMessageId());
            }
        } finally {
            wrapper.routeAfter(source);
        }
    }

    @Override
    public void block(ProduceMessage message) {
        Object source = wrapper.route(message);
        try {
            if (message.getBase().isStoreAtFailed()) {
                platform.update(SqlConstant.insertSQL, message.getMessageId(), new Timestamp(System.currentTimeMillis()), SerializerFactory.create().serialize(message.getBase()));
            } else {
                platform.update(SqlConstant.errorSQL, MESSAGE_BLOCKED.getCode(), new Timestamp(System.currentTimeMillis()), message.getMessageId());
            }
        } finally {
            wrapper.routeAfter(source);
        }
    }

    @Override
    public void report() {
        wrapper = WrapperFactory.match(this.dataSource);
        wrapper.report();
    }

    @Override
    public void beginTransaction() {
        wrapper.beginTransaction();
    }

    @Override
    public String dsIndex(boolean inTransaction) {
        return wrapper.dsIndex(inTransaction);
    }

    @Override
    public void endTransaction() {
        wrapper.endTransaction();
    }
}
