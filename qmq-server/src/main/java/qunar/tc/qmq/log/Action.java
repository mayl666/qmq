package qunar.tc.qmq.log;

/**
 * User: zhaohuiyu
 * Date: 1/8/13
 * Time: 10:48 AM
 */
public enum Action {
    /*
     * 正在接收消息
     */
    RECEIVING,
    /*
     * Broker收到消息
     */
    RECEIVED,
    /**
     * consumer开始拉消息
     */
    PULLING,
    /**
     * consumer拉到消息
     */
    PULLED
}
