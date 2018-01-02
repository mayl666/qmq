package qunar.tc.qmq;

import java.util.List;

/**
 * Created by zhaohui.yu
 * 15/12/8
 *
 * MessageListener如果实现了这个接口，则可以附加filter
 */
public interface FilterAttachable {
    List<Filter> filters();
}
