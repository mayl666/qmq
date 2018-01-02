package qunar.tc.qmq.protocol.consumer;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.utils.PayloadHolderUtils;

/**
 * @author yiqun.fan create on 17-8-25.
 */
public class AckRequestPayloadHolder implements PayloadHolder {
    private final AckRequest request;

    public AckRequestPayloadHolder(AckRequest request) {
        this.request = request;
    }

    @Override
    public void writeBody(ByteBuf out) {
        PayloadHolderUtils.writeString(request.getSubject(), out);
        PayloadHolderUtils.writeString(request.getGroup(), out);
        PayloadHolderUtils.writeString(request.getConsumerId(), out);
        out.writeLong(request.getPullOffsetBegin());
        out.writeLong(request.getPullOffsetLast());
    }
}
