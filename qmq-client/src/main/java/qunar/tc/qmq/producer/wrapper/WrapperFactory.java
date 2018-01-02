package qunar.tc.qmq.producer.wrapper;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper工厂。
 *
 * @author Daniel Li
 * @since 15 June 2015
 */
public class WrapperFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(WrapperFactory.class);

    public static DataSourceWrapper match(DataSource dataSource) {
        AbstractDataSourceWrapper[] wrappers = new AbstractDataSourceWrapper[4];
        wrappers[0] = create("qunar.tc.qmq.producer.wrapper.MatrixDataSourceWrapper", dataSource);
        wrappers[1] = create("qunar.tc.qmq.producer.wrapper.SharingDataSourceWrapper", dataSource);
        wrappers[2] = create("qunar.tc.qmq.producer.wrapper.GroupDataSourceWrapper", dataSource);
        wrappers[3] = create("qunar.tc.qmq.producer.wrapper.AtomicDataSourceWrapper", dataSource);

        for (AbstractDataSourceWrapper wrapper : wrappers) {
            if (wrapper != null && wrapper.support()) {
                LOGGER.info("Using Wrapper: {} process Transaction", wrapper.getClass());
                return wrapper;
            }
        }
        return new DefaultDataSourceWrapper(dataSource);
    }

    private static AbstractDataSourceWrapper create(String className, DataSource dataSource) {
        try {
            @SuppressWarnings("unchecked")
            Class<AbstractDataSourceWrapper> clazz = (Class<AbstractDataSourceWrapper>) Class.forName(className);
            return clazz.getConstructor(DataSource.class).newInstance(dataSource);
        } catch (Throwable t) {
            LOGGER.debug("Init class: {} failed. parameter: {}, message: {}", className, dataSource, t.getMessage());
            return null;
        }
    }
}
