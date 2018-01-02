package qunar.tc.qmq.configuration;

/**
 * User: zhaohuiyu Date: 12/24/12 Time: 4:12 PM
 */
public interface Config {
    String getString(String name);

    String getString(String name, String defaultValue);

    Integer getInt(String name);

    Integer getInt(String name, Integer defaultValue);

    Long getLong(String name);

    Long getLong(String name, Long defaultValue);

    Boolean getBoolean(String name, Boolean defaultValue);

    Boolean exists(String name);
}
