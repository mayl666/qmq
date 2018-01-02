package qunar.tc.qmq.service;

import qunar.tc.qmq.base.SubscribeRequest;

import java.util.Set;

/**
 * @author keli.wang
 * @since 2017/1/16
 */
public interface SubscribeService {
    void subscribe(final Set<SubscribeRequest> requests, final ConsumerMessageHandler handler);

    void unsubscribe(final Set<SubscribeRequest> requests);
}
