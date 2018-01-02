package qunar.tc.qmq.netty.exception;

/**
 * @author yiqun.fan create on 17-7-5.
 */
public class ClientSendException extends Exception {

    private static final long serialVersionUID = 6006709158339785244L;

    private final SendErrorCode sendErrorCode;
    private final String brokerAddr;

    public ClientSendException(SendErrorCode sendErrorCode) {
        super(sendErrorCode.name());
        this.sendErrorCode = sendErrorCode;
        this.brokerAddr = "";
    }

    public ClientSendException(SendErrorCode sendErrorCode, String brokerAddr) {
        super(sendErrorCode.name() + ", broker address [" + brokerAddr + "]");
        this.sendErrorCode = sendErrorCode;
        this.brokerAddr = brokerAddr;
    }

    public ClientSendException(SendErrorCode sendErrorCode, String brokerAddr, Throwable cause) {
        super(sendErrorCode.name() + ", broker address [" + brokerAddr + "]", cause);
        this.sendErrorCode = sendErrorCode;
        this.brokerAddr = brokerAddr;
    }

    public SendErrorCode getSendErrorCode() {
        return sendErrorCode;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public enum SendErrorCode {
        ILLEGAL_OPAQUE,
        EMPTY_ADDRESS,
        CREATE_CHANNEL_FAIL,
        CONNECT_BROKER_FAIL,
        WRITE_CHANNEL_FAIL,
        BROKER_BUSY
    }
}
