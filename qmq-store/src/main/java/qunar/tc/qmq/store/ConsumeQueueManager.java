package qunar.tc.qmq.store;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import java.util.List;

/**
 * @author keli.wang
 * @since 2017/7/31
 */
public class ConsumeQueueManager {
    private final Table<String, String, ConsumeQueue> queues;
    private final Table<String, String, MaxPulledMessageSequence> sequences;
    private final MessageStore store;

    public ConsumeQueueManager(final MessageStore store) {
        this.queues = HashBasedTable.create();
        this.sequences = create(store.allMaxPulledMessageSequences());
        this.store = store;
    }

    private Table<String, String, MaxPulledMessageSequence> create(final List<MaxPulledMessageSequence> sequences) {
        final HashBasedTable<String, String, MaxPulledMessageSequence> result = HashBasedTable.create();
        for (final MaxPulledMessageSequence sequence : sequences) {
            result.put(sequence.getSubject(), sequence.getGroup(), sequence);
        }
        return result;
    }

    public synchronized ConsumeQueue getOrCreate(final String subject, final String group) {
        if (!queues.contains(subject, group)) {
            queues.put(subject, group, new ConsumeQueue(store, subject, group, getLastMaxSequence(subject, group)));
        }
        return queues.get(subject, group);
    }

    private long getLastMaxSequence(final String subject, final String group) {
        if (!sequences.contains(subject, group)) {
            return -1;
        } else {
            return sequences.get(subject, group).getMaxSequence();
        }
    }
}
