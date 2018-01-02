package qunar.tc.qmq.utils;

import com.google.common.base.*;

import java.util.List;

/**
 * @author keli.wang
 * @since 2017/8/23
 */
public final class RetrySubjectUtils {
    private static final Joiner RETRY_SUBJECT_JOINER = Joiner.on('%');
    private static final Splitter RETRY_SUBJECT_SPLITTER = Splitter.on('%').trimResults().omitEmptyStrings();
    private static final String RETRY_SUBJECT_PREFIX = "%RETRY";
    private static final String DEAD_RETRY_SUBJECT_PREFIX = "%DEAD_RETRY";

    private RetrySubjectUtils() {
    }

    public static String buildRetrySubject(final String subject, final String group) {
        return RETRY_SUBJECT_JOINER.join(RETRY_SUBJECT_PREFIX, subject, group);
    }

    public static boolean isRetrySubject(final String subject) {
        return Strings.nullToEmpty(subject).startsWith(RETRY_SUBJECT_PREFIX);
    }

    public static String buildDeadRetrySubject(final String subject, final String group) {
        return RETRY_SUBJECT_JOINER.join(DEAD_RETRY_SUBJECT_PREFIX, subject, group);
    }

    public static boolean isDeadRetrySubject(final String subject) {
        return Strings.nullToEmpty(subject).startsWith(DEAD_RETRY_SUBJECT_PREFIX);
    }

    public static String getRealSubject(final String subject) {
        final Optional<String> optional = getSubject(subject);
        if (optional.isPresent()) {
            return optional.get();
        }
        return subject;
    }

    public static Optional<String> getSubject(final String retrySubject) {
        if (!isRetrySubject(retrySubject) && !isDeadRetrySubject(retrySubject)) {
            return Optional.absent();
        }
        final List<String> parts = RETRY_SUBJECT_SPLITTER.splitToList(retrySubject);
        if (parts.size() != 3) {
            return Optional.absent();
        } else {
            return Optional.of(parts.get(1));
        }
    }
}
