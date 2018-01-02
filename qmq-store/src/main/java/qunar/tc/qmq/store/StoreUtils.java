package qunar.tc.qmq.store;

import java.text.NumberFormat;

/**
 * @author keli.wang
 * @since 2017/7/3
 */
public final class StoreUtils {
    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }
}
