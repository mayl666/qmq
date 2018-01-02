package qunar.tc.qmq.utils;

import com.google.common.base.Joiner;

/**
 * @author keli.wang
 * @since 2017/8/23
 */
public final class DeadLetterQueueSubjectUtils {
    private static final Joiner DLQ_SUBJECT_JOINER = Joiner.on('%');
    private static final String DLQ_SUBJECT_PREFIX = "%DLQ";

    private DeadLetterQueueSubjectUtils() {
    }

    public static String buildDLQSubject(final String subject, final String group) {
        return DLQ_SUBJECT_JOINER.join(DLQ_SUBJECT_PREFIX, subject, group);
    }
}
