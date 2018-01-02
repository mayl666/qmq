package qunar.tc.qmq;

/**
 * Created by zhaohui.yu
 * 15/12/8
 * <p/>
 * 通过这个接口可以给MessageListener添加幂等检查的功能
 */
public interface IdempotentAttachable {
    IdempotentChecker getIdempotentChecker();
}
