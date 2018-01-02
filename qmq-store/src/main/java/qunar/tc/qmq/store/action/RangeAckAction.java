package qunar.tc.qmq.store.action;

import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.ActionType;

/**
 * @author yunfeng.yang
 * @since 2017/8/28
 */
public class RangeAckAction implements Action {
    private final String subject;
    private final String group;
    private final String consumerId;

    private final long firstSequence;
    private final long lastSequence;

    public RangeAckAction(String subject, String group, String consumerId, long firstSequence, long lastSequence) {
        this.subject = subject;
        this.group = group;
        this.consumerId = consumerId;

        this.firstSequence = firstSequence;
        this.lastSequence = lastSequence;
    }

    @Override
    public ActionType type() {
        return ActionType.RANGE_ACK;
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

    public long getFirstSequence() {
        return firstSequence;
    }

    public long getLastSequence() {
        return lastSequence;
    }
}
