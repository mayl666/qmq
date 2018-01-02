package qunar.tc.qmq.store.action;

import com.google.common.base.Preconditions;
import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.ActionType;

/**
 * @author keli.wang
 * @since 2017/8/20
 */
public class PullAction implements Action {
    private final String subject;
    private final String group;
    private final String consumerId;
    private final boolean broadcast;

    private final long firstSequence;
    private final long lastSequence;

    private final long firstMessageSequence;
    private final long lastMessageSequence;

    public PullAction(final String subject, final String group, final String consumerId, boolean broadcast,
                      long firstSequence, long lastSequence,
                      long firstMessageSequence, long lastMessageSequence) {
        Preconditions.checkArgument(lastSequence - firstSequence == lastMessageSequence - firstMessageSequence);

        this.subject = subject;
        this.group = group;
        this.consumerId = consumerId;
        this.broadcast = broadcast;

        this.firstSequence = firstSequence;
        this.lastSequence = lastSequence;

        this.firstMessageSequence = firstMessageSequence;
        this.lastMessageSequence = lastMessageSequence;
    }

    @Override
    public ActionType type() {
        return ActionType.PULL;
    }

    @Override
    public String subject() {
        return subject;
    }

    @Override
    public String group() {
        return group;
    }

    @Override
    public String consumerId() {
        return consumerId;
    }

    public boolean isBroadcast() {
        return broadcast;
    }

    public long getFirstSequence() {
        return firstSequence;
    }

    public long getLastSequence() {
        return lastSequence;
    }

    public long getFirstMessageSequence() {
        return firstMessageSequence;
    }

    public long getLastMessageSequence() {
        return lastMessageSequence;
    }
}
