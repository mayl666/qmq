package qunar.tc.qmq.netty.client;

/**
 * @author yiqun.fan create on 17-8-29.
 */
public interface HttpResponseCallback<V> {
    V onCompleted(Response response) throws Exception;

    void onThrowable(Throwable t);
}
