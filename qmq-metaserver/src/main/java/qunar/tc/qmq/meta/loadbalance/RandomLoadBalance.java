package qunar.tc.qmq.meta.loadbalance;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public class RandomLoadBalance extends AbstractLoadBalance {
    @Override
    List<String> doSelect(String subject, List<String> brokerGroups, int minNum) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final Set<String> resultSet = new HashSet<>(minNum);
        while (resultSet.size() < minNum) {
            final int randomIndex = random.nextInt(brokerGroups.size());
            resultSet.add(brokerGroups.get(randomIndex));
        }

        return new ArrayList<>(resultSet);
    }
}
