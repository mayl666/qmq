package qunar.tc.qmq.store;

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2017/8/20
 */
public interface ActionReaderWriter {
    int write(final ByteBuffer to, final Action action);

    Action read(final ByteBuffer from);
}
