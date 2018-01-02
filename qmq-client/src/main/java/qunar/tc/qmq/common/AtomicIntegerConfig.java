package qunar.tc.qmq.common;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yiqun.fan create on 17-8-17.
 */
public class AtomicIntegerConfig extends AtomicConfig<Integer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtomicIntegerConfig.class);

    private final int defaultValue;
    private final int minValue;
    private final int maxValue;

    public AtomicIntegerConfig(int defaultValue, int minValue, int maxValue) {
        this.defaultValue = defaultValue;
        this.minValue = Math.min(minValue, maxValue);
        this.maxValue = Math.max(minValue, maxValue);
    }

    @Override
    public Optional<Integer> parse(String key, String value) {
        int v = defaultValue;
        try {
            v = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOGGER.warn("parse config fail: {}={}, use default: {}", key, value, v);
        }
        if (v < minValue || v > maxValue) {
            LOGGER.warn("config value {} out of range: [{}, {}], use default: {}",
                    v, minValue, maxValue, defaultValue);
            v = defaultValue;
        }
        return Optional.of(v);
    }

    @Override
    public AtomicReference<Integer> get(String key) {
        return get(key, defaultValue);
    }
}
