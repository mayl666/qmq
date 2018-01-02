package qunar.tc.qmq.store;

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2017/7/25
 */
public class MessageLogRecord {
    private final int magic;
    private final byte attributes;
    private final long timestamp;
    private final String subject;
    private final ByteBuffer message;

    public MessageLogRecord(int magic, byte attributes, long timestamp, String subject, ByteBuffer message) {
        this.magic = magic;
        this.attributes = attributes;
        this.timestamp = timestamp;
        this.subject = subject;
        this.message = message;
    }

    public int getMagic() {
        return magic;
    }

    public byte getAttributes() {
        return attributes;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getSubject() {
        return subject;
    }

    public ByteBuffer getMessage() {
        return message;
    }
}
