package qunar.tc.qmq.store.action;

import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.store.MaxAckedPullLogSequence;
import qunar.tc.qmq.store.MaxPulledMessageSequence;
import qunar.tc.qmq.store.OffsetManager;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class MaxSequencesUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(MaxSequencesUpdater.class);

    private final OffsetManager manager;

    public MaxSequencesUpdater(final OffsetManager manager) {
        this.manager = manager;
    }

    @Subscribe
    public void onRangeAckAction(final RangeAckAction action) {
        final MaxAckedPullLogSequence sequence = manager.getOrCreateMaxAckedPullLogSequence(action.subject(), action.group(), action.consumerId());
        final long maxSequence = sequence.getMaxSequence();
        if (maxSequence >= action.getFirstSequence()) {
            LOG.warn("Maybe lost ack. Last acked sequence: {}. Current start acked sequence {}", maxSequence, action.getFirstSequence());
        }

        if (maxSequence < action.getLastSequence()) {
            sequence.setMaxSequence(action.getLastSequence());
        }
    }

    @Subscribe
    public void onPullAction(final PullAction action) {
        final MaxPulledMessageSequence sequence = manager.getOrCreateMaxPulledMessageSequence(action.subject(), action.group());
        final long maxSequence = sequence.getMaxSequence();
        if (maxSequence < action.getLastMessageSequence()) {
            sequence.setMaxSequence(action.getLastMessageSequence());
        }
    }
}
