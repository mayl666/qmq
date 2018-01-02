package qunar.tc.qmq.producer.tx;

import qunar.tc.qmq.MessageStore;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.TransactionListener;
import qunar.tc.qmq.TransactionProvider;
import qunar.tc.qmq.common.QmqLogger;

/**
 * Created by zhaohui.yu
 * 10/29/16
 */
public class MessageTracker {

    private final TransactionProvider transactionProvider;

    private final TransactionListener transactionListener;

    public MessageTracker(TransactionProvider transactionProvider) {
        this.transactionProvider = transactionProvider;
        this.transactionListener = new DefaultTransactionListener();
    }

    public boolean trackInTransaction(ProduceMessage message) {
        MessageStore messageStore = this.transactionProvider.messageStore();
        message.setStore(messageStore);
        if (transactionProvider.isInTransaction()) {
            this.transactionProvider.setTransactionListener(transactionListener);
            messageStore.beginTransaction();
            TransactionMessageHolder current = TransactionMessageHolder.init(messageStore);
            current.insertMessage(message);

            QmqLogger.log(message.getBase(), "事务持久消息");
            return true;
        } else {
            message.save();
            QmqLogger.log(message.getBase(), "持久消息");
            return false;
        }
    }
}
