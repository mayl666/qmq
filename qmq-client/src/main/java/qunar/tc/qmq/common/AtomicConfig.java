package qunar.tc.qmq.common;

import com.google.common.base.Optional;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class AtomicConfig<T> {

    private final ConcurrentMap<String, AtomicReference<T>> configMap = new ConcurrentHashMap<>();

    public void set(String key, T value) {
        AtomicReference<T> old = configMap.putIfAbsent(key, new AtomicReference<T>(value));
        if (old != null) {
            old.set(value);
        }
    }

    public void setString(String key, String value) {
        Optional<T> v = parse(key, value);
        if (v.isPresent()) {
            set(key, v.get());
        }
    }

    protected Optional<T> parse(String key, String value) {
        return Optional.absent();
    }

    public AtomicReference<T> get(String key) {
        return configMap.get(key);
    }

    public AtomicReference<T> get(String key, T defaultValue) {
        AtomicReference<T> tmp = new AtomicReference<>(defaultValue);
        AtomicReference<T> old = configMap.putIfAbsent(key, tmp);
        return old != null ? old : tmp;
    }

    public AtomicReference<T> get(String key, AtomicReference<T> defaultReference) {
        AtomicReference<T> old = configMap.putIfAbsent(key, defaultReference);
        return old != null ? old : defaultReference;
    }

    public Set<Map.Entry<String, AtomicReference<T>>> entrySet() {
        return configMap.entrySet();
    }
}
