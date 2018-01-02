package qunar.tc.qmq.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qconfig.client.Configuration;
import qunar.tc.qconfig.client.MapConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yiqun.fan create on 17-8-1.
 */
public class IntegerConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(IntegerConfig.class);

    private final int defaultValue;
    private final int minValue;
    private final int maxValue;
    private final ConcurrentHashMap<String, AtomicInteger> valueMap;

    public IntegerConfig(String filename, int defaultValue, int minValue, int maxValue) {
        this.defaultValue = defaultValue;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.valueMap = new ConcurrentHashMap<>();
        load(filename);
    }

    private void load(final String filename) {
        try {
            MapConfig.get(filename).addListener(new Configuration.ConfigListener<Map<String, String>>() {
                @Override
                public void onLoad(Map<String, String> conf) {
                    for (Map.Entry<String, String> entry : conf.entrySet()) {
                        try {
                            int value = Integer.parseInt(entry.getValue());
                            AtomicInteger ref = getValue(entry.getKey(), value);
                            ref.set(value);
                        } catch (Exception e) {
                            LOGGER.error("pull subject config error in file: {}. {}={}", filename, entry.getKey(), entry.getValue());
                        }
                    }
                }
            });
        } catch (Exception e) {
            LOGGER.info("can't get config file: {}. you can ignore it.", filename);
        }
    }

    public AtomicInteger getValue(String subjectPrefix) {
        return getValue(subjectPrefix, defaultValue);
    }

    public AtomicInteger getValue(String subjectPrefix, int defaultValue) {
        AtomicInteger temp = new AtomicInteger(Math.min(maxValue, Math.max(minValue, defaultValue)));
        AtomicInteger old = valueMap.putIfAbsent(subjectPrefix, temp);
        return old == null ? temp : old;
    }
}
