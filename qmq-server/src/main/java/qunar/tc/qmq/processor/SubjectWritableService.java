package qunar.tc.qmq.processor;

import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.Subscribe;
import qunar.tc.qmq.base.BrokerRegisterResponse;

/**
 * @author keli.wang
 * @since 2017/9/1
 */
public class SubjectWritableService {
    private volatile boolean readonly = false;
    private volatile ImmutableSet<String> writableSubjects = ImmutableSet.of();

    public boolean isReadonly() {
        return readonly;
    }

    public boolean isSubjectWritable(final String subject) {
        return writableSubjects.contains(subject);
    }

    @Subscribe
    public void onReadonlyUpdated(final Boolean online) {
        readonly = !online;
    }

    @Subscribe
    public void onWritableSubjectsUpdate(final BrokerRegisterResponse resp) {
        if (resp.getSubjects() == null) {
            writableSubjects = ImmutableSet.of();
        } else {
            writableSubjects = ImmutableSet.copyOf(resp.getSubjects());
        }
    }
}
