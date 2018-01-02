package qunar.tc.qmq.producer.tx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.MessageProducer;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.TransactionListener;

import java.util.List;
import java.util.Stack;

/**
 * Created by zhaohui.yu
 * 10/29/16
 */
class DefaultTransactionListener implements TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    private final ThreadLocal<Stack<TransactionMessageHolder>> resource = new ThreadLocal<Stack<TransactionMessageHolder>>() {
        @Override
        protected Stack<TransactionMessageHolder> initialValue() {
            return new Stack<>();
        }
    };

    @Override
    public void beforeCommit() {
        List<ProduceMessage> list = TransactionMessageHolder.get();
        if (list != null) {
            for (ProduceMessage msg : list) {
                msg.save();
            }
        }
    }

    @Override
    public void afterCommit() {
        List<ProduceMessage> list = TransactionMessageHolder.clear();
        if (list == null) return;

        for (int i = 0; i < list.size(); ++i) {
            ProduceMessage msg = list.get(i);
            try {
                msg.send();
            } catch (Throwable t) {
                logger.error("消息发送失败{}", msg.getMessageId(), t);
            }
        }
    }

    @Override
    public void afterCompletion() {
        List<ProduceMessage> list = TransactionMessageHolder.clear();
        if (list == null) return;

        for (int i = 0; i < list.size(); ++i) {
            ProduceMessage msg = list.get(i);
            logger.info("事务提交失败, 消息({})被忽略.subject:{}", msg.getMessageId(), msg.getSubject());
        }
    }

    @Override
    public void suspend() {
        resource.get().push(TransactionMessageHolder.suspend());
    }

    @Override
    public void resume() {
        TransactionMessageHolder.resume(resource.get().pop());
    }
}
