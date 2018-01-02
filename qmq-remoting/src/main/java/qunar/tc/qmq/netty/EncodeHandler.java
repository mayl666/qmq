package qunar.tc.qmq.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.RemotingHeader;

/**
 * @author yunfeng.yang
 * @since 2017/6/30
 */
public class EncodeHandler extends MessageToByteEncoder<Datagram> {

    //total|header size|header|body
    //total = len(header size) + len(header) + len(body)
    //header size = len(header)
    @Override
    protected void encode(ChannelHandlerContext ctx, Datagram msg, ByteBuf out) throws Exception {
        int start = out.writerIndex();
        int headerStart = start + RemotingHeader.LENGTH_FIELD;
        out.ensureWritable(RemotingHeader.LENGTH_FIELD);
        out.writerIndex(headerStart);

        final RemotingHeader header = msg.getHeader();
        encodeHeader(header, out);
        int headerSize = out.writerIndex() - headerStart;

        msg.writeBody(out);
        int end = out.writerIndex();
        int total = end - start - RemotingHeader.TOTAL_SIZE_LEN;

        out.writerIndex(start);
        out.writeInt(total);
        out.writeShort((short) headerSize);
        out.writerIndex(end);
    }

    private static void encodeHeader(final RemotingHeader header, final ByteBuf out) {
        out.writeInt(header.getMagicCode());
        out.writeShort(header.getCode());
        out.writeShort(header.getVersion());
        out.writeInt(header.getOpaque());
        out.writeInt(header.getFlag());
    }
}
