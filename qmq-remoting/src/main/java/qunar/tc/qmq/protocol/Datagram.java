package qunar.tc.qmq.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import qunar.api.pojo.node.JacksonSupport;

/**
 * @author yiqun.fan create on 17-7-4.
 */
public class Datagram {

    RemotingHeader header;
    private ByteBuf body;
    private PayloadHolder holder;

    public ByteBuf getBody() {
        return body;
    }

    public void setBody(ByteBuf body) {
        this.body = body;
    }

    public void setPayloadHolder(PayloadHolder holder) {
        this.holder = holder;
    }

    public RemotingHeader getHeader() {
        return header;
    }

    public void setHeader(RemotingHeader header) {
        this.header = header;
    }

    public void writeBody(ByteBuf out) {
        if (holder == null) return;
        holder.writeBody(out);
    }

    public void release() {
        ReferenceCountUtil.release(this.body);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("header", JacksonSupport.toJson(header))
                .toString();
    }
}
