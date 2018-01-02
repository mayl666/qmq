package qunar.tc.qmq.utils;

import qunar.api.pojo.node.JacksonSupport;

/**
 * @author yiqun.fan create on 17-7-6.
 */
public class JsonSerializeUtils {

    public static byte[] serialize(Object object) {
        return CharsetUtils.toUTF8Bytes(JacksonSupport.toJson(object));
    }
}
