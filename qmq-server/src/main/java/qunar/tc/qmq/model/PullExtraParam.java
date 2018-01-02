package qunar.tc.qmq.model;

/**
 * @author yunfeng.yang
 * @since 2017/8/23
 */
public class PullExtraParam {
    private final long pullLogOffset;
    private final long consumerLogOffset;

    public PullExtraParam(long pullLogOffset, long consumerLogOffset) {
        this.pullLogOffset = pullLogOffset;
        this.consumerLogOffset = consumerLogOffset;
    }

    public long getPullLogOffset() {
        return pullLogOffset;
    }

    public long getConsumerLogOffset() {
        return consumerLogOffset;
    }
}
