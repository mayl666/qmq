package qunar.tc.qmq.protocol;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.ReliabilityLevel;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.utils.CharsetUtils;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by zhaohui.yu
 * 7/21/17
 */
public class MessagesPayloadHolder implements PayloadHolder {

    private final List<BaseMessage> messages;

    public MessagesPayloadHolder(List<BaseMessage> messages) {
        this.messages = messages;
    }

    @Override
    public void writeBody(ByteBuf out) {
        if (messages == null || messages.size() == 0) return;
        for (BaseMessage message : messages) {
            serializeMessage(message, out);
        }
    }

    public static void serializeMessage(BaseMessage message, ByteBuf out) {
        int reliabilityLevel = message.getReliabilityLevel() == ReliabilityLevel.High ? 0 : 1;
        // flag
        out.writeByte((byte) reliabilityLevel);
        // created time
        out.writeLong(message.getCreatedTime().getTime());
        // expired time
        out.writeLong(message.getExpiredTime().getTime());
        // subject
        PayloadHolderUtils.writeString(message.getSubject(), out);
        // message id
        PayloadHolderUtils.writeString(message.getMessageId(), out);

        out.markWriterIndex();
        int writerIndex = out.writerIndex();
        int bodyStart = writerIndex + 4;
        out.ensureWritable(4);
        out.writerIndex(bodyStart);
        serializeMap(message.getAttrs(), out);
        int end = out.writerIndex();
        int bodyLen = end - bodyStart;
        out.resetWriterIndex();
        out.writeInt(bodyLen);
        out.writerIndex(end);
    }

    private static void serializeMap(Map<String, Object> map, ByteBuf out) {
        if (null == map || map.isEmpty()) return;

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) continue;
            byte[] keyBs = CharsetUtils.toUTF8Bytes(entry.getKey());
            byte[] valBs = CharsetUtils.toUTF8Bytes(entry.getValue().toString());

            out.writeShort((short) keyBs.length);
            out.writeBytes(keyBs);

            out.writeShort((short) valBs.length);
            out.writeBytes(valBs);
        }
    }
}
