package qunar.tc.qmq.producer.tx;

import com.google.common.base.Preconditions;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import qunar.tc.qmq.TransactionListener;
import qunar.tc.qmq.TransactionProvider;
import qunar.tc.qmq.MessageStore;

import javax.sql.DataSource;

/**
 * Created by zhaohui.yu
 * 10/26/16
 */
public class SpringTransactionProvider implements TransactionProvider, TransactionSynchronization {

    private static final RuntimeException E =
            new RuntimeException("当前开启了事务，但是事务管理器的transactionSynchronization设置为SYNCHRONIZATION_NEVER，与QMQ事务机制不兼容");

    private final MessageStore store;

    private TransactionListener transactionListener;

    public SpringTransactionProvider(DataSource bizDataSource) {
        this(bizDataSource, null);
    }

    public SpringTransactionProvider(DataSource bizDataSource, InstanceSingletonDataSource shardDataSource) {
        Preconditions.checkNotNull(bizDataSource, "业务数据源不能为空");
        this.store = createMessageStore(bizDataSource, shardDataSource);
        this.store.report();
    }

    private MessageStore createMessageStore(DataSource bizDataSource, InstanceSingletonDataSource shardDataSource) {
        if (shardDataSource != null) {
            return new MultiDataSourceMessageStore(bizDataSource, shardDataSource);
        }
        return new SingleDataSourceMessageStore(bizDataSource);
    }

    @Override
    public void suspend() {
        if (transactionListener != null) transactionListener.suspend();
    }

    @Override
    public void resume() {
        if (transactionListener != null) transactionListener.resume();
    }

    @Override
    public void flush() {

    }

    @Override
    public void beforeCommit(boolean readOnly) {
        if (readOnly) return;

        if (transactionListener != null) transactionListener.beforeCommit();
    }

    @Override
    public void beforeCompletion() {

    }

    @Override
    public void afterCommit() {
        if (transactionListener != null) transactionListener.afterCommit();
    }

    @Override
    public void afterCompletion(int status) {
        if (transactionListener != null) transactionListener.afterCompletion();
    }

    @Override
    public boolean isInTransaction() {
        return TransactionSynchronizationManager.isActualTransactionActive();
    }

    @Override
    public void setTransactionListener(TransactionListener listener) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) throw E;

        this.transactionListener = listener;
        TransactionSynchronizationManager.registerSynchronization(this);
    }

    @Override
    public MessageStore messageStore() {
        return this.store;
    }
}
