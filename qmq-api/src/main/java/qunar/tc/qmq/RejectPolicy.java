package qunar.tc.qmq;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * User: zhaohuiyu
 * Date: 8/12/13
 * Time: 6:52 PM
 * <p/>
 * consumer端队列满时拒绝策略
 */
@Deprecated
public enum RejectPolicy {
    /**
     * 取消，抛出异常，broker会立即重发消息(可靠消息)
     */
    Abort {
        @Override
        public RejectedExecutionHandler getHandler() {
            return new ThreadPoolExecutor.AbortPolicy();
        }
    },
    /**
     * 忽略当前消息，等待broker重发(可靠消息)
     */
    Discard {
        @Override
        public RejectedExecutionHandler getHandler() {
            return new ThreadPoolExecutor.DiscardPolicy();
        }
    },
    /**
     * 忽略最老的消息，等待broker重发(可靠消息)
     */
    DiscardOld {
        @Override
        public RejectedExecutionHandler getHandler() {
            return new ThreadPoolExecutor.DiscardOldestPolicy();
        }
    };

    public abstract RejectedExecutionHandler getHandler();
}
