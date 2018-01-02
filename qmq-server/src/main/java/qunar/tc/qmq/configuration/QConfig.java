package qunar.tc.qmq.configuration;

import qunar.tc.qconfig.client.MapConfig;

import java.util.Map;

/**
 * User: zhaohuiyu Date: 8/26/14 Time: 6:56 PM
 */
public class QConfig extends AbstractConfig {

    private final Map<String, String> config;

    public QConfig() {
        this.config = MapConfig.get("config.properties").asMap();
    }

    @Override
    protected String getProperty(String name) {
        return config.get(name);
    }
}
