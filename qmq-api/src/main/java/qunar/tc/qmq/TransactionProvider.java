package qunar.tc.qmq;

/**
 * Created by zhaohui.yu
 * 10/26/16
 */
public interface TransactionProvider {

    boolean isInTransaction();

    void setTransactionListener(TransactionListener listener);

    MessageStore messageStore();
}
