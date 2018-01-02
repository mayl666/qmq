package qunar.tc.qmq.utils;

import qunar.tc.qmq.model.PullExtraParam;
import qunar.tc.qmq.protocol.RemotingHeader;

import java.nio.ByteBuffer;

import static qunar.tc.qmq.protocol.RemotingHeader.*;

/**
 * @author yunfeng.yang
 * @since 2017/8/5
 */
public class PullRemoteHeaderSerializer {
    private static final short PULLING_OFFSET_LENGTH =
            8 // pull log begin offset
                    + 8; // consumer log begin offset

    public static ByteBuffer serializePullHeader(RemotingHeader header, PullExtraParam pullExtraParam, int messagesLength) {
        short headerLength = MIN_HEADER_SIZE;
        int bodyLen = PULLING_OFFSET_LENGTH + messagesLength;
        int total = HEADER_SIZE_LEN + headerLength + bodyLen;
        int bufferLength = TOTAL_SIZE_LEN + HEADER_SIZE_LEN + headerLength + PULLING_OFFSET_LENGTH;

        ByteBuffer pullHeaderBuffer = ByteBuffer.allocate(bufferLength);
        // total len
        pullHeaderBuffer.putInt(total);
        // header len
        pullHeaderBuffer.putShort(headerLength);
        // magic code
        pullHeaderBuffer.putInt(header.getMagicCode());
        // code
        pullHeaderBuffer.putShort(header.getCode());
        // version
        pullHeaderBuffer.putShort(header.getVersion());
        // opaque
        pullHeaderBuffer.putInt(header.getOpaque());
        // flag
        pullHeaderBuffer.putInt(header.getFlag());

        // the start of pull log offset
        pullHeaderBuffer.putLong(pullExtraParam.getPullLogOffset());
        // the start of consumer log offset
        pullHeaderBuffer.putLong(pullExtraParam.getConsumerLogOffset());

        pullHeaderBuffer.flip();
        return pullHeaderBuffer;
    }
}
