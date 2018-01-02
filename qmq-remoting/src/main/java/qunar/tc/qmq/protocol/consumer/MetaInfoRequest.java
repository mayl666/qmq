package qunar.tc.qmq.protocol.consumer;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class MetaInfoRequest {
    private String subject;
    private int clientTypeCode;

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public int getClientTypeCode() {
        return clientTypeCode;
    }

    public void setClientTypeCode(int clientTypeCode) {
        this.clientTypeCode = clientTypeCode;
    }

    @Override
    public String toString() {
        return "{" +
                "subject='" + subject + '\'' +
                ", clientTypeCode=" + clientTypeCode +
                '}';
    }
}
