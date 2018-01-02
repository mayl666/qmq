package qunar.tc.qmq.protocol;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.ReliabilityLevel;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.MessageHeader;
import qunar.tc.qmq.base.RawMessage;
import qunar.tc.qmq.protocol.consumer.AckRequest;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.producer.SendResult;
import qunar.tc.qmq.utils.CharsetUtils;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yunfeng.yang
 * @since 2017/7/14
 */
public class QMQSerializer {
    private static final int MESSAGE_MIN_LEN = 25;

    public static List<RawMessage> deserializeRawMessages(ByteBuf body) {
        if (body.readableBytes() == 0) {
            return Collections.emptyList();
        }

        List<RawMessage> messages = Lists.newArrayList();
        while (body.isReadable()) {
            RawMessage rawMessage = deserializeRawMessage(body);
            messages.add(rawMessage);
        }
        return messages;
    }

    public static RawMessage deserializeRawMessage(ByteBuf body) {
        body.markReaderIndex();
        byte flag = body.readByte();
        long createdTime = body.readLong();
        long expiredTime = body.readLong();

        int subjectLen = body.readShort();
        byte[] subjectBs = new byte[subjectLen];
        body.readBytes(subjectBs);
        String subject = CharsetUtils.toUTF8String(subjectBs);

        int messageIdLen = body.readShort();
        byte[] messageIdBs = new byte[messageIdLen];
        body.readBytes(messageIdBs);
        String messageId = CharsetUtils.toUTF8String(messageIdBs);

        MessageHeader header = new MessageHeader();
        header.setFlag(flag);
        header.setCreateTime(createdTime);
        header.setExpireTime(expiredTime);
        header.setSubject(subject);
        header.setMessageId(messageId);

        int bodyLen = body.readInt();

        int totalLen = MESSAGE_MIN_LEN + subjectLen + messageIdLen + bodyLen;
        body.resetReaderIndex();
        ByteBuf messageBuf = body.readSlice(totalLen);
        return new RawMessage(header, totalLen, messageBuf);
    }

    public static List<BaseMessage> deserializeBaseMessage(ByteBuf input) {
        if (input.readableBytes() == 0) return Collections.emptyList();
        List<BaseMessage> result = Lists.newArrayList();
        long pullLogOffset = input.readLong();
        long consumerLogOffset = input.readLong();

        while (input.isReadable()) {
            BaseMessage message = new BaseMessage();
            byte flag = input.readByte();
            long createdTime = input.readLong();
            long expiredTime = input.readLong();
            String subject = PayloadHolderUtils.readString(input);
            String messageId = PayloadHolderUtils.readString(input);

            byte[] bodyBs = PayloadHolderUtils.readBytes(input);
            HashMap<String, Object> attrs = deserializeMap(bodyBs);
            message.setMessageId(messageId);
            message.setSubject(subject);
            message.setAttrs(attrs);
            message.setExpiredTime(expiredTime);
            ReliabilityLevel reliabilityLevel = (flag & 1) == 0 ? ReliabilityLevel.High : ReliabilityLevel.Low;
            message.setReliabilityLevel(reliabilityLevel);
            message.setProperty(BaseMessage.keys.qmq_pullOffset, pullLogOffset);
            message.setProperty(BaseMessage.keys.qmq_consumerOffset, consumerLogOffset);
            result.add(message);

            pullLogOffset++;
            consumerLogOffset++;
        }
        return result;
    }

    public static PullRequest deserializePullRequest(ByteBuf input) {
        String prefix = PayloadHolderUtils.readString(input);
        String group = PayloadHolderUtils.readString(input);
        String consumerId = PayloadHolderUtils.readString(input);
        int requestNum = input.readInt();
        long offset = input.readLong();
        long pullOffsetBegin = input.readLong();
        long pullOffsetLast = input.readLong();
        long timeout = input.readLong();
        byte broadcast = input.readByte();

        PullRequest request = new PullRequest();
        request.setSubject(prefix);
        request.setGroup(group);
        request.setConsumerId(consumerId);
        request.setRequestNum(requestNum);
        request.setOffset(offset);
        request.setPullOffsetBegin(pullOffsetBegin);
        request.setPullOffsetLast(pullOffsetLast);
        request.setTimeoutMillis(timeout);
        request.setBroadcast(broadcast != 0);
        return request;
    }

    public static AckRequest deserializeAckRequest(ByteBuf input) {
        AckRequest request = new AckRequest();
        request.setSubject(PayloadHolderUtils.readString(input));
        request.setGroup(PayloadHolderUtils.readString(input));
        request.setConsumerId(PayloadHolderUtils.readString(input));
        request.setPullOffsetBegin(input.readLong());
        request.setPullOffsetLast(input.readLong());
        return request;
    }

    public static Map<String, SendResult> deserializeSendResultMap(ByteBuf buf) {
        Map<String, SendResult> result = Maps.newHashMap();
        while (buf.isReadable()) {
            int messageIdLen = buf.readShort();
            byte[] messageIdBs = new byte[messageIdLen];
            buf.readBytes(messageIdBs);
            String messageId = CharsetUtils.toUTF8String(messageIdBs);

            int code = buf.readInt();

            int remarkLen = buf.readShort();
            byte[] remarkBs = new byte[remarkLen];
            buf.readBytes(remarkBs);
            String remark = CharsetUtils.toUTF8String(remarkBs);

            result.put(messageId, new SendResult(code, remark));
        }
        return result;
    }

    private static HashMap<String, Object> deserializeMap(byte[] bytes) {
        if (bytes == null || bytes.length <= 0) return null;

        HashMap<String, Object> map = new HashMap<>();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        while (byteBuffer.hasRemaining()) {
            short keySize = byteBuffer.getShort();
            byte[] keyBs = new byte[keySize];
            byteBuffer.get(keyBs);

            short valSize = byteBuffer.getShort();
            byte[] valBs = new byte[valSize];
            byteBuffer.get(valBs);
            map.put(CharsetUtils.toUTF8String(keyBs), CharsetUtils.toUTF8String(valBs));
        }
        return map;
    }
}
