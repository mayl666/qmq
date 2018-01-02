package qunar.tc.qmq.common;

import com.google.common.base.Splitter;

/**
 * @author yiqun.fan create on 17-9-5.
 */
public class SplitterUtil {
    public static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
}
