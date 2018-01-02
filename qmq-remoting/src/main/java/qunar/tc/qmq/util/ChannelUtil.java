package qunar.tc.qmq.util;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * @author yiqun.fan create on 17-8-29.
 */
public class ChannelUtil {
    private static final AttributeKey<Object> DEFAULT_ATTRIBUTE = AttributeKey.valueOf("default");

    public static Object getAttribute(Channel channel) {
        synchronized (channel) {
            Attribute<Object> attr = channel.attr(DEFAULT_ATTRIBUTE);
            return attr != null ? attr.get() : null;
        }
    }

    public static boolean setAttributeIfAbsent(Channel channel, Object o) {
        synchronized (channel) {
            Attribute<Object> attr = channel.attr(DEFAULT_ATTRIBUTE);
            if (attr == null || attr.get() == null) {
                channel.attr(DEFAULT_ATTRIBUTE).set(o);
                return true;
            }
            return false;
        }
    }

    public static void removeAttribute(Channel channel) {
        synchronized (channel) {
            channel.attr(DEFAULT_ATTRIBUTE).set(null);
        }
    }
}
