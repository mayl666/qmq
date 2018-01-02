package qunar.tc.qmq.store.action;

import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.ActionReaderWriter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author keli.wang
 * @since 2017/8/20
 */
public class RangeAckActionReaderWriter implements ActionReaderWriter {
    @Override
    public int write(ByteBuffer to, Action action) {
        final int startIndex = to.position();

        final RangeAckAction rangeAck = (RangeAckAction) action;
        writeUTF8String(to, rangeAck.subject());
        writeUTF8String(to, rangeAck.group());
        writeUTF8String(to, rangeAck.consumerId());

        to.putLong(rangeAck.getFirstSequence());
        to.putLong(rangeAck.getLastSequence());

        return to.position() - startIndex;
    }

    private void writeUTF8String(final ByteBuffer to, final String s) {
        final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        to.putInt(bytes.length);
        to.put(bytes);
    }

    @Override
    public RangeAckAction read(final ByteBuffer from) {
        final String subject = readUTF8String(from);
        final String group = readUTF8String(from);
        final String consumerId = readUTF8String(from);

        final long firstSequence = from.getLong();
        final long lastSequence = from.getLong();

        return new RangeAckAction(subject, group, consumerId, firstSequence, lastSequence);
    }

    private String readUTF8String(final ByteBuffer from) {
        final int length = from.getInt();
        final byte[] bytes = new byte[length];
        from.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
