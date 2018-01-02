package qunar.tc.qmq.store.action;

import com.google.common.eventbus.Subscribe;
import qunar.tc.qmq.store.MessageStore;
import qunar.tc.qmq.store.PullLogMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class PullLogBuilder {
    private final MessageStore store;

    public PullLogBuilder(final MessageStore store) {
        this.store = store;
    }

    @Subscribe
    public void onPullAction(final PullAction action) {
        if (!action.isBroadcast()) {
            store.putPullLogs(action.subject(), action.group(), action.consumerId(), createMessages(action));
        }
    }

    private List<PullLogMessage> createMessages(final PullAction action) {
        final int count = (int) (action.getLastSequence() - action.getFirstSequence() + 1);
        final List<PullLogMessage> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            messages.add(new PullLogMessage(action.getFirstSequence() + i, action.getFirstMessageSequence() + i));
        }

        return messages;
    }
}
