package qunar.tc.qmq.store;

import java.util.ArrayList;
import java.util.List;

/**
 * @author keli.wang
 * @since 2017/7/6
 */
public class GetMessageResult {
    private final List<SelectSegmentBufferResult> segmentBuffers = new ArrayList<>(100);
    private int bufferTotalSize = 0;

    private GetMessageStatus status;
    private long minOffset;
    private long maxOffset;
    private long nextBeginOffset;

    private OffsetRange consumerLogRange;

    public GetMessageResult() {
    }

    public GetMessageResult(GetMessageStatus status) {
        this.status = status;
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public List<SelectSegmentBufferResult> getSegmentBuffers() {
        return segmentBuffers;
    }

    public void addSegmentBuffer(final SelectSegmentBufferResult segmentBuffer) {
        segmentBuffers.add(segmentBuffer);
        bufferTotalSize += segmentBuffer.getSize();
    }

    public int getMessageNum() {
        return segmentBuffers.size();
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public OffsetRange getConsumerLogRange() {
        return consumerLogRange;
    }

    public void setConsumerLogRange(OffsetRange consumerLogRange) {
        this.consumerLogRange = consumerLogRange;
    }
}
