package qunar.tc.qmq.store;

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2017/7/6
 */
public class SelectSegmentBufferResult {
    private final long startOffset;
    private final ByteBuffer buffer;
    private final int size;

    private long wroteOffset;
    private int wroteBytes;
    private long payloadOffset;

    public SelectSegmentBufferResult(long startOffset, ByteBuffer buffer, int size) {
        this.startOffset = startOffset;
        this.buffer = buffer;
        this.size = size;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int getSize() {
        return size;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }

    public long getPayloadOffset() {
        return payloadOffset;
    }

    public void setPayloadOffset(long payloadOffset) {
        this.payloadOffset = payloadOffset;
    }
}
