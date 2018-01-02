package qunar.tc.qmq.serializer.impl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.serializer.Serializer;

import java.io.IOException;

/**
 * User: zhaohuiyu
 * Date: 12/15/13
 * Time: 12:49 AM
 */
public class JacksonSerializer implements Serializer {
    private static final Logger logger = LoggerFactory.getLogger(JacksonSerializer.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.disable(SerializationFeature.INDENT_OUTPUT);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        mapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        mapper.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, true);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    @Override
    public String serialize(Object data) {
        try {
            return mapper.writeValueAsString(data);
        } catch (IOException e) {
            logger.error("serialize data error", e);
            return null;
        }
    }

    @Override
    public byte[] serializeToBytes(Object data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (IOException e) {
            logger.error("serialize data error", e);
            return null;
        }
    }

    @Override
    public <T> T deSerialize(String content, Class<T> clazz) {
        try {
            return mapper.readValue(content, clazz);
        } catch (IOException e) {
            logger.error("deserialize object error: {}", content, e);
            return null;
        }
    }

    @Override
    public <T> T deSerialize(byte[] content, Class<T> clazz) {
        try {
            return mapper.readValue(content, clazz);
        } catch (IOException e) {
            logger.error("deserialize object error: {}", content, e);
            return null;
        }
    }

    @Override
    public <T> T deSerialize(byte[] content, TypeReference typeReference) {
        try {
            return mapper.readValue(content, typeReference);
        } catch (IOException e) {
            logger.error("deserialize object error: {}", content, e);
            return null;
        }
    }

    @Override
    public <T> T deSerialize(String content, TypeReference typeReference) {
        try {
            return mapper.readValue(content, typeReference);
        } catch (IOException e) {
            logger.error("deserialize object error: {}", content, e);
            return null;
        }
    }
}
