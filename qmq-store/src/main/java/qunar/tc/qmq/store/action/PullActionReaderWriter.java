package qunar.tc.qmq.store.action;

import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.ActionReaderWriter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * @author keli.wang
 * @since 2017/8/20
 */
public class PullActionReaderWriter implements ActionReaderWriter {
    private static final byte TRUE_BYTE = (byte) 1;
    private static final byte FALSE_BYTE = (byte) 0;

    @Override
    public int write(final ByteBuffer to, final Action action) {
        final int startIndex = to.position();

        final PullAction pull = (PullAction) action;
        writeUTF8String(to, pull.subject());
        writeUTF8String(to, pull.group());
        writeUTF8String(to, pull.consumerId());

        to.put(toByte(pull.isBroadcast()));

        to.putLong(pull.getFirstSequence());
        to.putLong(pull.getLastSequence());

        to.putLong(pull.getFirstMessageSequence());
        to.putLong(pull.getLastMessageSequence());

        return to.position() - startIndex;
    }

    @Override
    public PullAction read(final ByteBuffer from) {
        final String subject = readUTF8String(from);
        final String group = readUTF8String(from);
        final String consumerId = readUTF8String(from);

        final boolean broadcast = fromByte(from.get());

        final long firstSequence = from.getLong();
        final long lastSequence = from.getLong();

        final long firstMessageSequence = from.getLong();
        final long lastMessageSequence = from.getLong();

        return new PullAction(subject, group, consumerId, broadcast, firstSequence, lastSequence, firstMessageSequence, lastMessageSequence);
    }

    private void writeUTF8String(final ByteBuffer to, final String s) {
        final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        to.putInt(bytes.length);
        to.put(bytes);
    }

    private String readUTF8String(final ByteBuffer from) {
        final int length = from.getInt();
        final byte[] bytes = new byte[length];
        from.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private byte toByte(final boolean bool) {
        return bool ? TRUE_BYTE : FALSE_BYTE;
    }

    private boolean fromByte(final byte b) {
        return Objects.equals(b, TRUE_BYTE);
    }
}
