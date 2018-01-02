package qunar.tc.qmq.producer.wrapper;

import qunar.tc.qmq.ProduceMessage;

/**
 * 数据源包装。
 *
 * @author Daniel Li
 * @since 15 June 2015
 */
public interface DataSourceWrapper {

    void report();

    void beginTransaction();

    void endTransaction();

    Object route(ProduceMessage message);

    /**
     * 和{@link qunar.tc.qmq.producer.wrapper.DataSourceWrapper#route(qunar.tc.qmq.producer.ProduceMessageImpl)}一起使用，实现类似callback功能。
     * @see qunar.tc.qmq.producer.wrapper.DataSourceWrapper#route(qunar.tc.qmq.producer.ProduceMessageImpl)
     */
    void routeAfter(Object source);

    String dsIndex(boolean inTransaction);
}
