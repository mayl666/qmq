package qunar.tc.qmq.utils;

import java.nio.ByteBuffer;

/**
 * @author yunfeng.yang
 * @since 2017/8/5
 */
public final class MessageUtils {
    private MessageUtils() {
    }

    public static boolean isHigh(ByteBuffer buffer) {
        buffer.mark();
        byte flag = buffer.get();
        buffer.reset();
        return (flag & 1) == 0;
    }
}
