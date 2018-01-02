package qunar.tc.qmq.utils;

/**
 * @author yunfeng.yang
 * @since 2017/7/31
 */
public final class ActorUtils {
    private ActorUtils() {
    }

    public static String pullActorPath(String subject, String group) {
        return subject + "-" + group;
    }
}
