package qunar.tc.qmq.producer.tx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * User: zhaohuiyu
 * Date: 8/12/14
 * Time: 5:04 PM
 */
class PXCDataSourceExtractorV12 {
    private static final Logger logger = LoggerFactory.getLogger(PXCDataSourceExtractorV12.class);

    public static String extract(DataSource dataSource) {
        try {
            String namespace = ((com.qunar.db.resource.ReadWriteDataSource) dataSource).getNamespace();
            return JdbcUtils.PROTOCOL_PXC + "://" + namespace;
        } catch (Throwable e) {
            logger.error("获取PXC的namespace失败，请联系TCDEV", e);
            throw new RuntimeException(e);
        }
    }

    public static boolean isPxcDataSource(DataSource ds) {
        return ds.getClass().getCanonicalName().equals("com.qunar.db.resource.ReadWriteDataSource");
    }
}
