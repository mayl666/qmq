package qunar.tc.qmq.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Created by zhaohui.yu
 * 7/21/17
 */
public interface PayloadHolder {
    void writeBody(ByteBuf out);
}
