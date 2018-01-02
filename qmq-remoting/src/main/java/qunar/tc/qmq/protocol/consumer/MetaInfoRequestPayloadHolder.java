package qunar.tc.qmq.protocol.consumer;

import io.netty.buffer.ByteBuf;
import qunar.api.pojo.node.JacksonSupport;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.utils.CharsetUtils;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class MetaInfoRequestPayloadHolder implements PayloadHolder {
    private final MetaInfoRequest request;

    public MetaInfoRequestPayloadHolder(MetaInfoRequest request) {
        this.request = request;
    }

    @Override
    public void writeBody(ByteBuf out) {
        out.writeBytes(CharsetUtils.toUTF8Bytes(JacksonSupport.toJson(request)));
    }
}
