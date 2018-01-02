package qunar.tc.qmq.metainfoclient;

import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.consumer.MetaInfoResponse;

/**
 * @author yiqun.fan create on 17-9-1.
 */
public interface MetaInfoClient {
    void sendRequest(MetaInfoRequest request);

    void registerResponseSubscriber(ResponseSubscriber receiver);

    interface ResponseSubscriber {
        void onResponse(MetaInfoResponse response);
    }
}
