package qunar.tc.qmq.monitor;

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
    private static final String[] prefixAndGroup = new String[]{"subjectPrefix", "consumerGroup"};
    private static final String[] subjectArr = new String[]{"subject"};
    private static final String[] consumerIdArr = new String[]{"consumerId"};

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

    private static void countInc(String name, String subject) {
        countInc(name, subjectArr, new String[]{subject});
    }

    private static void countInc(String name, String subjectPrefix, String consumerGroup) {
        countInc(name, prefixAndGroup, new String[]{subjectPrefix, consumerGroup});
    }

    private static void countInc(String name, String[] values, int num) {
        countInc(name, prefixAndGroup, values, num);
    }

    private static void countInc(String name, String[] tags, String[] values) {
        cacheFor(new CounterKey(name, tags, values)).inc();
    }

    private static void countInc(String name, String[] tags, String[] values, int num) {
        cacheFor(new CounterKey(name, tags, values)).inc(num);
    }

    public static void produceTime(String subject, long time) {
        cacheFor(new TimerKey("produceTime", subjectArr, new String[]{subject})).update(time, TimeUnit.MILLISECONDS);
    }

    public static void receivedMessagesCountInc(String subject) {
        String[] values = {subject};

        cacheFor(new MeterKey("receivedMessagesEx", subjectArr, values)).mark();
    }

    public static void pulledMessagesCountInc(String subject, String group, int messageNum) {
        String[] values = new String[]{subject, group};

        cacheFor(new MeterKey("pulledMessagesEx", prefixAndGroup, values)).mark(messageNum);
    }

    public static void pulledNoMessagesCountInc(String subject, String group) {
        countInc("pulledNoMessagesCount", subject, group);
    }

    public static void storeMessageErrorCountInc(String subject) {
        countInc("storeMessageErrorCount", subject);
    }

    public static void suspendRequestCountInc(String subject, String group) {
        countInc("suspendRequestCount", subject, group);
    }

    public static void resumeActorCountInc(String subject, String group) {
        countInc("resumeActorCount", subject, group);
    }

    public static void pullTimeOutCountInc(String subject, String group) {
        countInc("pullTimeOutCount", subject, group);
    }

    public static void getMessageErrorCountInc(String subject, String group) {
        countInc("getMessageErrorCount", subject, group);
    }

    public static void consumerAckCountInc(String subject, String group, int size) {
        countInc("consumerAckCount", new String[]{subject, group}, size);
    }

    public static void consumerLostAckCountInc(String subject, String group, int lostAckCount) {
        countInc("consumerLostAckCount", new String[]{subject, group}, lostAckCount);
    }

    public static void connectionInActiveErrorCountInc(String consumerId) {
        countInc("connectionErrorCount", consumerIdArr, new String[]{consumerId});
    }

    public static void putMessageTime(String subject, long time) {
        cacheFor(new TimerKey("putMessageTime", subjectArr, new String[]{subject})).update(time, TimeUnit.MILLISECONDS);
    }

    public static void processTime(String subject, long time) {
        cacheFor(new TimerKey("processTime", subjectArr, new String[]{subject})).update(time, TimeUnit.MILLISECONDS);
    }

    public static void rejectReceivedMessageCountInc(String subject) {
        countInc("rejectReceivedMessageCount", subject);
    }

    public static void brokerBusyMessageCountInc(String subject) {
        countInc("brokerBusyMessageCount", subject);
    }

    public static void brokerReadOnlyMessageCountInc(String subject) {
        countInc("brokerReadOnlyMessageCount", subject);
    }

    public static void receivedFailedCountInc(String subject) {
        countInc("receivedFailedCount", subject);
    }

    public static void expiredMessagesCountInc(String subject) {
        countInc("expiredMessagesCount", subject);
    }

    public static void slaveSyncLogOffset(String logType, long diff) {
        cacheFor(new TimerKey("slaveSyncLogOffsetLag", new String[]{"logType"}, new String[]{logType})).update(diff, TimeUnit.MILLISECONDS);
    }

    public static void pullProcessTime(String subject, String group, long elapsed) {
        cacheFor(new TimerKey("pullProcessTime", prefixAndGroup, new String[]{subject, group})).update(elapsed, TimeUnit.MILLISECONDS);
    }

    private static abstract class Key<M> {
        public String name;
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
