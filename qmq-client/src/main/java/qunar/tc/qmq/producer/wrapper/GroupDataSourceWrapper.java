package qunar.tc.qmq.producer.wrapper;

import java.util.Arrays;
import java.util.Set;

import javax.sql.DataSource;

import qunar.tc.qclient.db.common.loadevent.LoadEvent;
import qunar.tc.qclient.db.common.loadevent.LoadListener;
import qunar.tc.qclient.db.common.loadevent.LoadListenerController;
import qunar.tc.qclient.db.jdbc.client.support.QDBGroupDataSource;
import qunar.tc.qclient.db.jdbc.group.AbstractGroupDataSource;
import qunar.tc.qmq.producer.sender.PositionSpy;
import qunar.tc.qmq.producer.tx.JdbcUtils;

import com.google.common.collect.Sets;

/**
 * Group数据源包装器。
 *
 * @author Daniel Li
 * @since 15 June 2015
 */
class GroupDataSourceWrapper extends AbstractDataSourceWrapper {

    public GroupDataSourceWrapper(DataSource dataSource) {
        super(dataSource, dataSource instanceof AbstractGroupDataSource || dataSource instanceof QDBGroupDataSource);
    }

    @Override
    public void report() {
        LoadListener<LoadEvent> listener = new LoadListener<LoadEvent>() {

            private Set<String> notifiedKeys = Sets.newHashSet();

            @Override
            public void onLoad(LoadEvent loadEvent) {
                // 会有异常，不做处理
                String url = JdbcUtils.getDatabaseInstanceURL(dataSource);
                String key = JdbcUtils.extractKey(url);
                synchronized (notifiedKeys) {
                    if (!notifiedKeys.contains(key)) {
                        PositionSpy.notify(Arrays.asList(key));
                        notifiedKeys.add(key);
                    }
                }
            }
        };

        ((LoadListenerController<LoadEvent>) dataSource).addLoadListener(listener);
    }
}
