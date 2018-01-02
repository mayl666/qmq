package qunar.tc.qmq.common;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class ClientData {
    private String id;
    private long consumeOffset;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getConsumeOffset() {
        return consumeOffset;
    }

    public void setConsumeOffset(long consumeOffset) {
        this.consumeOffset = consumeOffset;
    }
}
