/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.producer.tx;

import qunar.tc.qmq.MessageStore;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.ReliabilityLevel;

import java.util.ArrayList;
import java.util.List;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-5
 */
class TransactionMessageHolder {

    private static final ThreadLocal<TransactionMessageHolder> holder = new ThreadLocal<TransactionMessageHolder>();

    static TransactionMessageHolder init(MessageStore store) {
        TransactionMessageHolder old = holder.get();
        if (old != null)
            return old;
        TransactionMessageHolder current = new TransactionMessageHolder(store);
        holder.set(current);
        return current;
    }

    public void insertMessage(ProduceMessage message) {
        if (ReliabilityLevel.isLow(message.getBase())) return;

        TransactionMessageHolder container = holder.get();
        if (container == null) return;
        container.add(message);
    }

    static TransactionMessageHolder suspend() {
        TransactionMessageHolder resource = holder.get();
        holder.remove();
        return resource;
    }

    static void resume(TransactionMessageHolder resource) {
        holder.set(resource);
    }

    static List<ProduceMessage> clear() {
        TransactionMessageHolder container = holder.get();
        if (container == null)
            return null;
        container.store.endTransaction();
        holder.remove();
        return container.queue;
    }

    static List<ProduceMessage> get() {
        TransactionMessageHolder container = holder.get();
        if (container == null)
            return null;
        return container.queue;
    }

    private final MessageStore store;
    private List<ProduceMessage> queue;

    private TransactionMessageHolder(MessageStore store) {
        this.store = store;
    }

    private void add(ProduceMessage message) {
        if (queue == null)
            queue = new ArrayList<>(1);

        queue.add(message);
        message.setStore(store);
        message.setDsIndex(store.dsIndex(true));
    }

}
