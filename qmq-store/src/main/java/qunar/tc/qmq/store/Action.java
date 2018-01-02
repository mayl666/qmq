package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2017/8/20
 */
public interface Action {
    ActionType type();

    String subject();

    String group();

    String consumerId();
}
