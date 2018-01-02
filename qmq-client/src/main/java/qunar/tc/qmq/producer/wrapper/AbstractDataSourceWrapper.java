package qunar.tc.qmq.producer.wrapper;

import qunar.tc.qmq.ProduceMessage;

import javax.sql.DataSource;

/**
 * 抽象数据源包装。
 *
 * @author Daniel Li
 * @since 15 June 2015
 */
abstract class AbstractDataSourceWrapper implements DataSourceWrapper {

    protected DataSource dataSource;

    private final boolean support;

    public AbstractDataSourceWrapper(DataSource dataSource, boolean support) {
        this.dataSource = dataSource;
        this.support = support;
    }

    public final boolean support() {
        return support;
    }

    @Override
    public void beginTransaction() {

    }

    @Override
    public void endTransaction() {

    }

    @Override
    public Object route(ProduceMessage message) {
        return null;
    }

    @Override
    public void routeAfter(Object source) {

    }

    @Override
    public String dsIndex(boolean inTransaction) {
        return null;
    }
}
