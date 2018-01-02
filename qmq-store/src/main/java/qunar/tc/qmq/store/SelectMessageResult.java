package qunar.tc.qmq.store;

import io.netty.buffer.ByteBuf;

/**
 * @author yunfeng.yang
 * @since 2017/7/6
 */
public class SelectMessageResult {
    private ByteBuf messages;
    private int latestOffset;

    public ByteBuf getMessages() {
        return messages;
    }

    public int getLatestOffset() {
        return latestOffset;
    }
}
