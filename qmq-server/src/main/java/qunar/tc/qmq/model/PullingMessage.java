package qunar.tc.qmq.model;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author yunfeng.yang
 * @since 2017/8/7
 */
public class PullingMessage {
    private final ByteBuffer message;
    private final Map<String, String> additionalProperties;

    public PullingMessage(ByteBuffer message, Map<String, String> additionalProperties) {
        this.message = message;
        this.additionalProperties = additionalProperties;
    }

    public ByteBuffer getMessage() {
        return message;
    }

    public Map<String, String> getAdditionalProperties() {
        return additionalProperties;
    }
}
