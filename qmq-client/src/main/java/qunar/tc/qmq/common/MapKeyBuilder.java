package qunar.tc.qmq.common;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class MapKeyBuilder {
    private static final String SEPARATOR = "$";

    private static final Splitter SPLITTER = Splitter.on(SEPARATOR).trimResults().omitEmptyStrings();

    public static String buildSubscribeKey(String subject, String group) {
        return Strings.nullToEmpty(subject) + SEPARATOR + Strings.nullToEmpty(group);
    }

    public static String buildSenderKey(String brokerGroupName, String subject, String group) {
        return brokerGroupName + MapKeyBuilder.SEPARATOR + buildSubscribeKey(subject, group);
    }

    public static String buildMetaInfoKey(ClientType clientType, String subject) {
        return clientType.name() + MapKeyBuilder.SEPARATOR + subject;
    }

    public static List<String> splitKey(String key) {
        return Strings.isNullOrEmpty(key) ? new ArrayList<String>() : SPLITTER.splitToList(key);
    }
}
