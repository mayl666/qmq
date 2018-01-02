package qunar.tc.qmq.consumer.pull;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * @author yiqun.fan create on 17-9-12.
 */
class PullConsumerImpl implements PullConsumer {
    private final String subject;
    private final String group;
    private final PullService pullService = PullService.getInstance();

    PullConsumerImpl(String subject, String group) {
        this.subject = subject;
        this.group = group;
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
    public List<PulledMessage> pull(int batchSize) {
        Preconditions.checkArgument(batchSize > 0, "batchSize must > 0");
        return null;
    }
}
