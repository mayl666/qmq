package qunar.tc.qmq.processor.filters;

import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.processor.Invoker;

import java.util.ArrayList;
import java.util.List;

/**
 * User: zhaohuiyu Date: 4/2/13 Time: 12:14 PM
 */
public class ReceiveFilterChain implements Disposable {
    private final List<ReceiveFilter> filters = new ArrayList<>();

    public ReceiveFilterChain() {
        addFilter(new ValidateFilter());
        addFilter(new ExpiredMessageFilter());
    }

    public Invoker buildFilterChain(Invoker invoker) {
        Invoker last = invoker;
        if (filters.size() > 0) {
            for (int i = filters.size() - 1; i >= 0; i--) {
                final ReceiveFilter filter = filters.get(i);
                final Invoker next = last;
                last = message -> filter.invoke(next, message);
            }
        }
        return last;
    }

    private ReceiveFilterChain addFilter(ReceiveFilter filter) {
        filters.add(filter);
        return this;
    }

    @Override
    public void destroy() {
        if (filters != null && !filters.isEmpty()) {
            for (ReceiveFilter filter : filters) {
                if (filter instanceof Disposable) {
                    ((Disposable) filter).destroy();
                }
            }
        }
    }
}

