package qunar.tc.qmq.protocol.consumer;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.utils.PayloadHolderUtils;

/**
 * @author yiqun.fan create on 17-8-2.
 */
public class PullRequestPayloadHolder implements PayloadHolder {
    private final PullRequest request;

    public PullRequestPayloadHolder(PullRequest request) {
        this.request = request;
    }

    @Override
    public void writeBody(ByteBuf out) {
        PayloadHolderUtils.writeString(request.getSubject(), out);
        PayloadHolderUtils.writeString(request.getGroup(), out);
        PayloadHolderUtils.writeString(request.getConsumerId(), out);
        out.writeInt(request.getRequestNum());
        out.writeLong(request.getOffset());
        out.writeLong(request.getPullOffsetBegin());
        out.writeLong(request.getPullOffsetLast());
        out.writeLong(request.getTimeoutMillis());
        out.writeByte(request.isBroadcast() ? 1 : 0);
    }
}
