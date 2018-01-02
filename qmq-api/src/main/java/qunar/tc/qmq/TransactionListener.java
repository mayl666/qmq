package qunar.tc.qmq;

/**
 * Created by zhaohui.yu
 * 10/29/16
 */
public interface TransactionListener {

    void beforeCommit();

    void afterCommit();

    void afterCompletion();

    void suspend();

    void resume();
}
