package qunar.tc.qmq.consumer.pull;

import java.util.List;

/**
 * @author yiqun.fan create on 17-9-11.
 */
public interface PullConsumer {

    String subject();

    String group();

    List<PulledMessage> pull(int batchSize);
}
