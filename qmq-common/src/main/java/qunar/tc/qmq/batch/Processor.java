package qunar.tc.qmq.batch;

import java.util.List;

/**
 * User: zhaohuiyu
 * Date: 6/4/13
 * Time: 5:21 PM
 */
public interface Processor<Item> {
    void process(List<Item> items);
}
