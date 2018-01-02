package qunar.tc.qmq.store;

import com.google.common.eventbus.EventBus;
import qunar.tc.qmq.store.action.MaxSequencesUpdater;
import qunar.tc.qmq.store.action.PullLogBuilder;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class ActionLogDispatchService {
    private final EventBus dispatcher;
    private final OffsetManager offsetManager;

    public ActionLogDispatchService(final MessageStoreConfig config, final MessageStore store) {
        this.dispatcher = new EventBus("action-log-dispatcher");
        this.offsetManager = new OffsetManager(config);

        register(new MaxSequencesUpdater(offsetManager));
        register(new PullLogBuilder(store));
    }

    private void register(final Object handler) {
        dispatcher.register(handler);
    }

    public void dispatch(final Action action) {
        dispatcher.post(action);
    }

    public OffsetManager getOffsetManager() {
        return offsetManager;
    }
}
