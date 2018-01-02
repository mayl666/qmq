package qunar.tc.qmq.web.service;

/**
 * @author yunfeng.yang
 * @since 2017/7/11
 */
public interface ProducerTestService {
    void start(double qps, String subject, final int len, final boolean isLow);

    void stop();
}
