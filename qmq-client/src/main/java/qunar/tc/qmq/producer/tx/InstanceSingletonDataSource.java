/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.producer.tx;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-8
 */
public class InstanceSingletonDataSource extends AbstractRoutingDataSource {

    private static final ThreadLocal<String> holder = new ThreadLocal<String>();

    private volatile String defaultTargetKey;

    private Map<Object, Object> databases;

    public void setDataBaseURL(String jdbcURL) {
        if (!Strings.isNullOrEmpty(defaultTargetKey)) {
            holder.set(defaultTargetKey);
            return;
        }

        String key = JdbcUtils.extractKey(jdbcURL);
        if (!databases.containsKey(key)) {
            throw new IllegalStateException("要查找的URL不存在:" + key);
        }

        holder.set(key);
    }

    public void setDatabaseInstances(List<DataSource> list) {
        databases = new HashMap<Object, Object>();

        if (list.size() == 1) {
            DataSource ds = list.get(0);
            String url = JdbcUtils.getDatabaseInstanceURL(ds);
            String key = JdbcUtils.extractKey(url);
            databases.put(key, ds);
            defaultTargetKey = key;
            super.setTargetDataSources(databases);
            return;
        }

        for (DataSource ds : list) {
            String url = JdbcUtils.getDatabaseInstanceURL(ds);
            String key = JdbcUtils.extractKey(url);
            if (databases.containsKey(key)) {
                throw new DuplicateKeyException("Datasouce 重复, 同一个数据库实例只需要设置一个datasouce. key: " + key);
            }
            databases.put(key, ds);
        }
        super.setTargetDataSources(databases);
    }

    public List<String> keys() {
        List<String> keys = Lists.newArrayListWithCapacity(databases.size());
        for (Object key : databases.keySet())
            keys.add(key.toString());
        return keys;
    }

    @Override
    protected Object determineCurrentLookupKey() {
        return holder.get();
    }
}
