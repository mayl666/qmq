package qunar.tc.qmq.consumer.pull;

/**
 * @author yiqun.fan create on 17-9-11.
 */
public interface AckHook {
    void call(PulledMessage message, Throwable throwable);
}
