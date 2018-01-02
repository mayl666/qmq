package qunar.tc.qmq.base;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

/**
 * @author yiqun.fan create on 17-7-4.
 */
public class RawMessage {
    private final MessageHeader header;

    private final int messageSize;
    private final ByteBuf messageBuf;

    public RawMessage(MessageHeader header, int messageSize, ByteBuf messageBuf) {
        this.header = header;
        this.messageSize = messageSize;
        this.messageBuf = messageBuf.duplicate().retain();
    }

    public MessageHeader getHeader() {
        return header;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public ByteBuf getMessageBuf() {
        return messageBuf;
    }

    public void setSubject(String subject) {
        header.setSubject(subject);
    }

    public boolean isHigh() {
        return (header.getFlag() & 1) == 0;
    }

    public void release() {
        ReferenceCountUtil.release(this.messageBuf);
    }
}
