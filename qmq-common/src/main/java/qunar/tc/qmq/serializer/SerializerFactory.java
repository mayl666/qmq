package qunar.tc.qmq.serializer;

import qunar.tc.qmq.serializer.impl.JacksonSerializer;

/**
 * User: zhaohuiyu
 * Date: 2/26/14
 * Time: 10:37 AM
 */
public class SerializerFactory {

    private static final Serializer DEFAULT = new JacksonSerializer();

    public static Serializer create() {
        return DEFAULT;
    }
}
