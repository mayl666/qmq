package qunar.tc.qmq.store;

import com.google.common.collect.ImmutableMap;
import qunar.tc.qmq.store.action.PullActionReaderWriter;
import qunar.tc.qmq.store.action.RangeAckActionReaderWriter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author keli.wang
 * @since 2017/8/20
 */
public enum ActionType {
    PULL((byte) 0, new PullActionReaderWriter()),
    RANGE_ACK((byte) 1, new RangeAckActionReaderWriter());

    private static final ImmutableMap<Byte, ActionType> INSTANCES;

    static {
        final Map<Byte, ActionType> instances = new HashMap<>();
        for (final ActionType t : values()) {
            instances.put(t.getCode(), t);
        }
        INSTANCES = ImmutableMap.copyOf(instances);
    }

    private final byte code;
    private final ActionReaderWriter readerWriter;

    ActionType(final byte code, final ActionReaderWriter readerWriter) {
        this.code = code;
        this.readerWriter = readerWriter;
    }

    public static ActionType fromCode(final byte code) {
        if (INSTANCES.containsKey(code)) {
            return INSTANCES.get(code);
        }

        throw new RuntimeException("unknown action type code");
    }

    public byte getCode() {
        return code;
    }

    public ActionReaderWriter getReaderWriter() {
        return readerWriter;
    }
}
