package qunar.tc.qmq.store;

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2017/7/5
 */
public interface MessageAppender<T, R> {
    AppendMessageResult<R> doAppend(final long baseOffset, final ByteBuffer targetBuffer, final int freeSpace, final T message);
}
