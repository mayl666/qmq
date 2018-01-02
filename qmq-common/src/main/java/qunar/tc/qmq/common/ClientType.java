package qunar.tc.qmq.common;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yiqun.fan create on 17-8-22.
 */
public enum ClientType {
    PRODUCER(1),
    CONSUMER(2),
    OTHER(3);

    private static final ImmutableMap<Integer, ClientType> INSTANCES;

    static {
        final Map<Integer, ClientType> result = new HashMap<>();
        for (final ClientType type : values()) {
            result.put(type.getCode(), type);
        }
        INSTANCES = ImmutableMap.copyOf(result);
    }

    private int code;

    ClientType(int code) {
        this.code = code;
    }

    public static ClientType of(final int code) {
        return INSTANCES.get(code);
    }

    public int getCode() {
        return code;
    }
}
