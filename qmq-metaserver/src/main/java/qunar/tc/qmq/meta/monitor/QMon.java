package qunar.tc.qmq.meta.monitor;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import qunar.metrics.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author yunfeng.yang
 * @since 2017/7/10
 */
public final class QMon {
    private static LoadingCache<Key, Object> CACHE = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build(new CacheLoader<Key, Object>() {
        @Override
        public Object load(Key key) throws Exception {
            return key.create();
        }
    });

    @SuppressWarnings("unchecked")
    private static <M> M cacheFor(Key<M> key) {
        return (M) CACHE.getUnchecked(key);
    }

    private static void countInc(String name, String[] tags, String[] values) {
        cacheFor(new CounterKey(name, tags, values)).inc();
    }

    public static void brokerRegisterCountInc(String groupName, int requestType) {
        countInc("brokerRegisterCount", new String[]{"groupName", "requestType"}, new String[]{groupName, String.valueOf(requestType)});
    }

    public static void brokerDisconnectedCountInc(String groupName) {
        countInc("brokerDisconnectedCount", new String[]{"groupName"}, new String[]{groupName});
    }

    public static void clientRegisterCountInc(String subject, int clientTypeCode) {
        countInc("clientRegisterCount", new String[]{"subject", "clientTypeCode"}, new String[]{subject, String.valueOf(clientTypeCode)});
    }

    public static void clientSubjectRouteCountInc(String subject) {
        countInc("clientSubjectRouteCount", new String[]{"subject"}, new String[]{subject});
    }

    private static abstract class Key<M> {
        String name;
        String[] tags;
        String[] values;

        Key(String name, String[] tags, String[] values) {
            this.name = name;
            this.tags = tags;
            this.values = values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key) o;

            if (name != null ? !name.equals(key.name) : key.name != null)
                return false;
            if (!Arrays.equals(tags, key.tags))
                return false;
            if (!Arrays.equals(values, key.values))
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (tags != null ? Arrays.hashCode(tags) : 0);
            result = 31 * result + (values != null ? Arrays.hashCode(values) : 0);
            return result;
        }

        public abstract M create();
    }

    private static class CounterKey extends Key<Counter> {

        CounterKey(String name, String[] tags, String[] values) {
            super(name, tags, values);
        }

        @Override
        public Counter create() {
            DeltaKeyWrapper<Counter> key = Metrics.counter(name);
            for (int i = 0; i < tags.length; ++i) {
                key = key.tag(tags[i], values[i]);
            }

            return key.delta().get();
        }
    }

    private static class MeterKey extends Key<Meter> {

        MeterKey(String name, String[] tags, String[] values) {
            super(name, tags, values);
        }

        @Override
        public Meter create() {
            KeyWrapper<Meter> key = Metrics.meter(name);
            for (int i = 0; i < tags.length; ++i) {
                key = key.tag(tags[i], values[i]);
            }
            return key.get();
        }
    }

    private static class TimerKey extends Key<Timer> {

        TimerKey(String name, String[] tags, String[] values) {
            super(name, tags, values);
        }

        @Override
        public Timer create() {
            KeyWrapper<Timer> key = Metrics.timer(name);
            for (int i = 0; i < tags.length; ++i) {
                key = key.tag(tags[i], values[i]);
            }
            return key.get();
        }
    }
}
