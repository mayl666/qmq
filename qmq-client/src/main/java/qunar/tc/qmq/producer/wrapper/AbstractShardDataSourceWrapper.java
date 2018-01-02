package qunar.tc.qmq.producer.wrapper;

import com.alibaba.dubbo.common.URL;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import qunar.tc.qclient.db.client.ThreadLocalString;
import qunar.tc.qclient.db.client.util.ThreadLocalMap;
import qunar.tc.qclient.db.common.loadevent.LoadEvent;
import qunar.tc.qclient.db.common.loadevent.LoadListener;
import qunar.tc.qclient.db.common.loadevent.LoadListenerController;
import qunar.tc.qclient.db.jdbc.client.support.QDBMatrixDataSource;
import qunar.tc.qclient.db.jdbc.client.support.QDBSharingDataSource;
import qunar.tc.qclient.db.jdbc.sharing.AbstractSharingConnection;
import qunar.tc.qclient.db.jdbc.sharing.AbstractSharingDataSource;
import qunar.tc.qclient.db.jdbc.sharing.rule.ExecutionPlan;
import qunar.tc.qclient.db.jdbc.sharing.rule.RouteHelper;
import qunar.tc.qclient.db.matrix.jdbc.AbstractMatrixConnection;
import qunar.tc.qclient.db.matrix.jdbc.AbstractMatrixDataSource;
import qunar.tc.qclient.db.matrix.jdbc.ConnectionManager;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.SqlConstant;
import qunar.tc.qmq.producer.sender.PositionSpy;
import qunar.tc.qmq.producer.tx.JdbcUtils;

import javax.sql.DataSource;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.util.Iterator;
import java.util.Map;

/**
 * 抽象分片数据源包装。
 *
 * @author Daniel Li
 * @since 15 June 2015
 */
abstract class AbstractShardDataSourceWrapper extends AbstractDataSourceWrapper {

    protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public AbstractShardDataSourceWrapper(DataSource dataSource, boolean support) {
        super(dataSource, support);
    }

    protected final Map<Connection, String> connectionMap = Maps.newConcurrentMap();

    protected final Map<String, String> dsIndexMap = Maps.newConcurrentMap();

    @Override
    public void report() {
        LoadListener<LoadEvent> listener = new LoadListener<LoadEvent>() {

            @Override
            public void onLoad(LoadEvent loadEvent) {
                // 会有异常，不做处理
                String qclientDbUrl = JdbcUtils.getDatabaseInstanceURL(loadEvent.getCurrentDataSource());
                Map<String, String> dsIndexMap = decode(URL.valueOf(qclientDbUrl).getParameters());

                // 只会不断增加，不会递减
                if (!dsIndexMap.isEmpty()) {
                    MapDifference<String, String> difference = Maps.difference(dsIndexMap, AbstractShardDataSourceWrapper.this.dsIndexMap);
                    if (!difference.entriesOnlyOnLeft().isEmpty()) {
                        AbstractShardDataSourceWrapper.this.dsIndexMap.putAll(difference.entriesOnlyOnLeft());
                        PositionSpy.notify(Lists.newArrayList(difference.entriesOnlyOnLeft().values()));
                    }
                }
            }
        };
        ((LoadListenerController<LoadEvent>) dataSource).addLoadListener(listener);
    }

    protected Map<String, String> decode(Map<String, String> dsIndexMap) {
        Map<String, String> result = Maps.newHashMapWithExpectedSize(dsIndexMap.size());
        for (Map.Entry<String, String> entry : dsIndexMap.entrySet()) {
            try {
                String value = JdbcUtils.extractKey(URLDecoder.decode(entry.getValue(), "UTF-8"));
                result.put(entry.getKey(), value);
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("URL parameter: {} is not support {}", dsIndexMap, "UTF-8", e);
            }
        }
        return result;
    }

    @Override
    public Object route(ProduceMessage message) {
        if (message.getDsIndex() == null) {
            String dbIndex = null;
            Connection connection = getConnection();
            if (connection != null) {
                dbIndex = connectionMap.get(connection);
            }
            if (dbIndex == null) {
                dbIndex = getDefaultDbIndex();
            }
            message.setDsIndex(dbIndex);
        }
        return doRoute(message.getDsIndex());
    }

    protected abstract Object doRoute(String dsIndex);

    @Override
    public void endTransaction() {
        Connection connection = getConnection();
        if (connection != null) {
            connectionMap.remove(connection);
        }
    }

    @Override
    public String dsIndex(boolean inTransaction) {
        if (inTransaction) {
            Connection connection = getConnection();
            return connection == null ? null : connectionMap.get(connection);
        } else {
            return getDefaultDbIndex();
        }
    }

    protected String getDefaultDbIndex() {
        Iterator<String> iterator = dsIndexMap.keySet().iterator();
        return iterator.next();                // 没有应该抛出异常
    }

    protected Connection getConnection() {
        try {
            ConnectionHolder conHolder = (ConnectionHolder) TransactionSynchronizationManager.getResource(dataSource);
            return conHolder.getConnection();
        } catch (Exception e) {
            LOGGER.warn("Cann't get current connection. message: {}", e.getMessage(), e);
            return null;
        }
    }
}

/**
 * Sharing数据源包装。
 *
 * @author Daniel Li
 * @since 15 June 2015
 */
class SharingDataSourceWrapper extends AbstractShardDataSourceWrapper {

    public SharingDataSourceWrapper(DataSource dataSource) {
        // TODO 缺少client包应该没问题，需要确认一下
        super(dataSource, dataSource instanceof AbstractSharingDataSource || dataSource instanceof QDBSharingDataSource);
    }

    @Override
    public void beginTransaction() {
        final Connection connection = getConnection();
        if (connection != null) {
            ((AbstractSharingConnection) connection).addChangeListener(new AbstractSharingConnection.ChangeListener() {
                @Override
                public void onChange(AbstractSharingConnection.ChangeEvent changeEvent) {
                    if (!changeEvent.isAutoCommit()) {
                        connectionMap.put(connection, changeEvent.getDbIndex());
                    }
                }
            });
        }
    }

    public Object doRoute(String dsIndex) {
        Object source = ThreadLocalMap.get(ThreadLocalString.DATASOURCE_SELECTOR_POINTOR);
        RouteHelper.executeByExecutionPlan(new ExecutionPlan(dsIndex, SqlConstant.DATABASE_NAME));
        return source;
    }

    @Override
    public void routeAfter(Object source) {
        ThreadLocalMap.put(ThreadLocalString.DATASOURCE_SELECTOR_POINTOR, source);
    }
}

/**
 * Matrix数据源包装。
 *
 * @author Daniel Li
 * @since 15 June 2015
 */
class MatrixDataSourceWrapper extends AbstractShardDataSourceWrapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(MatrixDataSourceWrapper.class);

    public MatrixDataSourceWrapper(DataSource dataSource) {
        super(dataSource, dataSource instanceof AbstractMatrixDataSource || dataSource instanceof QDBMatrixDataSource);
    }

    @Override
    public void beginTransaction() {
        final Connection connection = getConnection();
        if (connection != null) {
            ((AbstractMatrixConnection) connection).addChangeListener(new ConnectionManager.ChangeListener() {

                @Override
                public void onChange(ConnectionManager.ChangeEvent event) {
                    if (!event.isAutoCommit()) {
                        connectionMap.put(connection, event.getDbIndex());
                    }
                }
            });
        }
    }

    public Object doRoute(String dsIndex) {
        Object source = ThreadLocalMap.get(ThreadLocalString.DB_SELECTOR);
        qunar.tc.qclient.db.matrix.util.RouteHelper.executeByDB(dsIndex);
        return source;
    }

    @Override
    public void routeAfter(Object source) {
        ThreadLocalMap.put(ThreadLocalString.DB_SELECTOR, source);
    }
}
