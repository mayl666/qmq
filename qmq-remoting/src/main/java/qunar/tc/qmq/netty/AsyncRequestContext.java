package qunar.tc.qmq.netty;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author keli.wang
 * @since 2017/2/27
 */
public class AsyncRequestContext {
    private static final ThreadLocal<AsyncRequestContext> LOCAL = new ThreadLocal<AsyncRequestContext>() {
        @Override
        protected AsyncRequestContext initialValue() {
            return new AsyncRequestContext();
        }
    };

    private ListenableFuture<?> future;

    private AsyncRequestContext() {
    }

    public static AsyncRequestContext getContext() {
        return LOCAL.get();
    }

    ListenableFuture<?> getFuture() {
        return future;
    }

    public void setFuture(ListenableFuture<?> future) {
        this.future = future;
    }

    static void removeContext() {
        LOCAL.remove();
    }
}
