package qunar.tc.qmq.producer.wrapper;

import com.google.common.collect.Sets;
import qunar.tc.qclient.db.common.loadevent.LoadListener;
import qunar.tc.qclient.db.jdbc.atom.AbstractQAtomDataSource;
import qunar.tc.qclient.db.jdbc.atom.config.DbStatus;
import qunar.tc.qclient.db.jdbc.atom.support.AtomicLoadEvent;
import qunar.tc.qmq.producer.sender.PositionSpy;
import qunar.tc.qmq.producer.tx.JdbcUtils;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.Set;

/**
 * Atom数据源包装器。
 *
 * @author Daniel Li
 * @since 15 June 2015
 */
class AtomicDataSourceWrapper extends AbstractDataSourceWrapper {

    public AtomicDataSourceWrapper(DataSource dataSource) {
        super(dataSource, dataSource instanceof AbstractQAtomDataSource);
    }

    @Override
    public void report() {
        LoadListener<AtomicLoadEvent> listener = new LoadListener<AtomicLoadEvent>() {

            private Set<String> notifiedKeys = Sets.newHashSet();

            @Override
            public void onLoad(AtomicLoadEvent loadEvent) {
                if (loadEvent.getDbStatus() != DbStatus.NA) {
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
            }
        };

        ((AbstractQAtomDataSource) dataSource).addLoadListener(listener);
    }
}
