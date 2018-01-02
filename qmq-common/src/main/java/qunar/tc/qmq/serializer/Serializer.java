package qunar.tc.qmq.serializer;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * User: zhaohuiyu
 * Date: 7/27/13
 * Time: 2:50 PM
 */
public interface Serializer {
    String serialize(Object data);

    byte[] serializeToBytes(Object data);

    <T> T deSerialize(String content, Class<T> clazz);

    <T> T deSerialize(byte[] content, Class<T> clazz);

    <T> T deSerialize(byte[] content, TypeReference typeReference);

    <T> T deSerialize(String content, TypeReference typeReference);
}
