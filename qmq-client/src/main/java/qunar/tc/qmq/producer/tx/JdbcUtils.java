package qunar.tc.qmq.producer.tx;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.springframework.jdbc.support.DatabaseMetaDataCallback;
import org.springframework.jdbc.support.MetaDataAccessException;

import javax.sql.DataSource;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * User: zhaohuiyu Date: 5/23/13 Time: 6:49 PM
 */
public class JdbcUtils {

    public static String getDatabaseInstanceURL(DataSource ds) {
        if (ds instanceof LookupKeyDeterminable) {
            return ((LookupKeyDeterminable) ds).currentKey();
        }
        if (PXCDataSourceExtractorV12.isPxcDataSource(ds)) {
            return PXCDataSourceExtractorV12.extract(ds);
        }

        if (PXCDataSourceExtractorV13.isPxcDataSource(ds)) {
            return PXCDataSourceExtractorV13.extract(ds);
        }
        try {
            return (String) org.springframework.jdbc.support.JdbcUtils.extractDatabaseMetaData(ds, extractor);
        } catch (MetaDataAccessException e) {
            throw new RuntimeException("获取metadata出错，请检查数据库权限! qmq与业务共享数据源，业务库的账号需要对qmq_produce库有CURD权限", e);
        }
    }

    private static DatabaseMetaDataCallback extractor = new DatabaseMetaDataCallback() {
        @Override
        public Object processMetaData(DatabaseMetaData dbmd) throws SQLException, MetaDataAccessException {
            return dbmd.getURL();
        }
    };

    // jdbc:mysql://192.168.28.46:3306/otappb?useUnicode=true&amp;characterEncoding=UTF-8&amp;zeroDateTimeBehavior=convertToNull
    // -->
    // jdbc:mysql://192.168.28.46:3306

    private static final String PROTOCOL_MYSQL = "jdbc:mysql";
    private static final String PROTOCOL_POSTGRESQL = "jdbc:postgresql";
    static final String PROTOCOL_PXC = "jdbc:pxc";
    private static final String PROTOCOL_SQLSERVER = "jdbc:sqlserver";

    public static String extractKey(String jdbcUrl) {
        jdbcUrl = Preconditions.checkNotNull(jdbcUrl);
        URL url = URL.valueOf(jdbcUrl);
        String result = null;
        String protocol = url.getProtocol().toLowerCase();
        if (PROTOCOL_MYSQL.equals(protocol)) {
            result = url.getProtocol() + "://" + url.getAddress();
        } else if (PROTOCOL_POSTGRESQL.equals(protocol)) {
            String db = url.getPath();
            result = url.getProtocol() + "://" + url.getAddress() + (Strings.isNullOrEmpty(db) ? "" : "/" + db);
        } else if (PROTOCOL_PXC.equals(protocol)) {
            result = jdbcUrl;
        } else if (PROTOCOL_SQLSERVER.equals(protocol)) {
            result = url.getProtocol() + "://" + url.getAddress() + ";databaseName=" + url.getParameter("databaseName");
        } else {
            throw new IllegalArgumentException("jdbcUrl 必须以 jdbc:mysql,jdbc:postgresql,jdbc:pxc或jdbc:sqlserver开头,jdbcUrl=" + jdbcUrl);
        }
        return result;
    }
}
