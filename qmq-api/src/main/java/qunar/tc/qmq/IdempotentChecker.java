package qunar.tc.qmq;

/**
 * Created by zhaohui.yu
 * 15/11/4
 * <p/>
 * 幂等检查
 * <p/>
 * 已经提供了
 *
 * @see qunar.tc.qmq.consumer.idempotent.RedisIdempotentChecker
 * @see qunar.tc.qmq.consumer.idempotent.JdbcIdempotentChecker
 * @see qunar.tc.qmq.consumer.idempotent.TransactionalJdbcIdempotentChecker
 * <p/>
 * 如果不能满足需求，最好从
 * @see qunar.tc.qmq.consumer.idempotent.AbstractIdempotentChecker
 * 派生
 */
public interface IdempotentChecker {

    /**
     * 消息是否已经处理过
     *
     * @param message 投递过来的消息
     * @return 是否已经处理
     */
    boolean isProcessed(Message message);

    /**
     * 标记消息是否处理
     *
     * @param message 消息
     * @param e       消费消息是否出异常
     */
    void markProcessed(Message message, Throwable e);
}
