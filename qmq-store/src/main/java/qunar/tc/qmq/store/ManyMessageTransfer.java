package qunar.tc.qmq.store;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * @author keli.wang
 * @since 2017/7/6
 */
public class ManyMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer headerBuffer;
    private final List<SelectSegmentBufferResult> segmentBuffers;
    private final int bufferTotalSize;

    private long transferred;

    public ManyMessageTransfer(ByteBuffer headerBuffer, List<SelectSegmentBufferResult> segmentBuffers, int bufferTotalSize) {
        this.headerBuffer = headerBuffer;
        this.segmentBuffers = segmentBuffers;
        this.bufferTotalSize = bufferTotalSize;
    }

    @Override
    public long position() {
        int pos = headerBuffer.position();
        for (SelectSegmentBufferResult segmentBuffer : segmentBuffers) {
            pos += segmentBuffer.getBuffer().position();
        }
        return pos;
    }

    @Override
    public long transfered() {
        return transferred;
    }

    @Override
    public long count() {
        return headerBuffer.limit() + bufferTotalSize;
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.headerBuffer.hasRemaining()) {
            transferred += target.write(this.headerBuffer);
            return transferred;
        } else {
            for (SelectSegmentBufferResult segmentBuffer : segmentBuffers) {
                final ByteBuffer buffer = segmentBuffer.getBuffer();
                if (buffer.hasRemaining()) {
                    transferred += target.write(buffer);
                    return transferred;
                }
            }
        }

        return 0;
    }

    public void close() {
        this.deallocate();
    }

    @Override
    protected void deallocate() {
//        this.getMessageResult.release();
    }
}
