package qunar.tc.qmq.producer.wrapper;

import java.util.Arrays;

import javax.sql.DataSource;

import qunar.tc.qmq.producer.sender.PositionSpy;
import qunar.tc.qmq.producer.tx.JdbcUtils;

/**
 * 默认数据源包装。
 *
 * @author Daniel Li
 * @since 15 June 2015
 */
class DefaultDataSourceWrapper extends AbstractDataSourceWrapper {

    public DefaultDataSourceWrapper(DataSource dataSource) {
        super(dataSource, true);
    }

    @Override
    public void report() {
        String url = JdbcUtils.getDatabaseInstanceURL(dataSource);
        String key = JdbcUtils.extractKey(url);
        PositionSpy.notify(Arrays.asList(key));
    }
}
