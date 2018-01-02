package qunar.tc.qmq.configuration;

import org.apache.commons.lang3.StringUtils;

/**
 * User: zhaohuiyu Date: 1/9/13 Time: 12:09 AM
 */
public abstract class AbstractConfig implements Config {

    @Override
    public String getString(String name) {
        return getPropertyWithCheck(name);
    }

    @Override
    public String getString(String name, String defaultValue) {
        String property = getProperty(name);
        if (StringUtils.isBlank(property))
            return defaultValue;
        return property;
    }

    @Override
    public Integer getInt(String name) {
        return Integer.valueOf(getPropertyWithCheck(name));
    }

    @Override
    public Integer getInt(String name, Integer defaultValue) {
        String property = getProperty(name);
        if (StringUtils.isBlank(property))
            return defaultValue;
        return Integer.valueOf(property);
    }

    @Override
    public Long getLong(String name) {
        return Long.valueOf(getPropertyWithCheck(name));
    }

    @Override
    public Long getLong(String name, Long defaultValue) {
        String property = getProperty(name);
        if (StringUtils.isBlank(property))
            return defaultValue;
        return Long.valueOf(property);
    }

    @Override
    public Boolean getBoolean(String name, Boolean defaultValue) {
        String property = getProperty(name);
        if (StringUtils.isBlank(property))
            return defaultValue;
        return Boolean.valueOf(property);
    }

    @Override
    public Boolean exists(String name) {
        String property = getProperty(name);
        if (StringUtils.isBlank(property))
            return Boolean.FALSE;
        return Boolean.TRUE;
    }

    private String getPropertyWithCheck(String name) {
        String property = getProperty(name);
        if (StringUtils.isBlank(property)) {
            throw new RuntimeException("配置项: " + name + " 获取失败，服务启动终止");
        }
        return property;
    }

    protected abstract String getProperty(String name);
}
