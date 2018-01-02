package qunar.tc.qmq.meta.store;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import qunar.agile.Conf;
import qunar.tc.datasourceconfigprovider.DataSourceConfigProvider;
import qunar.tc.qconfig.client.MapConfig;

import javax.sql.DataSource;

/**
 * User: zhaohuiyu Date: 12/25/12 Time: 10:56 AM
 */
class DatabaseUtils {

    static DataSource createDataSource(String config) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(config), "dataSource config can not be null or empty");

        final Conf conf = Conf.fromMap(MapConfig.get(config).asMap());

        final DataSourceConfigProvider provider = new DataSourceConfigProvider();
        provider.setDbname(conf.getString("jdbc.dbname", ""));
        provider.setUrl(conf.getString("jdbc.url", ""));
        provider.setUsername(conf.getString("jdbc.username", ""));
        provider.setPassword(conf.getString("jdbc.password", ""));

        final int poolSize = conf.getInt("poolSize", 10);
        final boolean defaultAutoCommit = conf.getBoolean("defaultAutoCommit", true);

        final HikariConfig cpConfig = new HikariConfig();
        cpConfig.setAutoCommit(defaultAutoCommit);
        cpConfig.setDriverClassName(provider.getDriverClass());
        cpConfig.setJdbcUrl(provider.getUrl());
        cpConfig.setUsername(provider.getUsername());
        cpConfig.setPassword(provider.getPassword());
        cpConfig.setMaximumPoolSize(poolSize);

        return new HikariDataSource(cpConfig);
    }
}