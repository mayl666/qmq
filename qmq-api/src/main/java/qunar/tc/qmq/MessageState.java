package qunar.tc.qmq;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-26
 */
public class MessageState implements Serializable {
    private static final long serialVersionUID = 8121694126433314973L;

    private static final Map<Integer, MessageState> MAP = new HashMap<Integer, MessageState>();

    public static final MessageState VALID_ERROR = new MessageState(0);
    public static final MessageState INIT = new MessageState(100);
    public static final MessageState PRODUCER_SENDING = new MessageState(200);
    public static final MessageState PRODUCER_SENT = new MessageState(300);

    public static final MessageState BROKER_DELAY_RECEIVED = new MessageState(320);
    public static final MessageState BROKER_DELAY_QUEUE = new MessageState(340);
    public static final MessageState BROKER_DELAY_SENTING = new MessageState(360);
    public static final MessageState BROKER_DELAY_SENT = new MessageState(380);
    public static final MessageState BROKER_RECEIVED = new MessageState(400);
    public static final MessageState BROKER_SENDING = new MessageState(500);
    public static final MessageState BROKER_SENT = new MessageState(600);

    public static final MessageState CONSUMER_ACK = new MessageState(700);

    /**
     * 转移至历史表
     */
    public static final MessageState TRANSFERED = new MessageState(800);
    public static final MessageState DEAD_RESEND = new MessageState(900);

    public static final MessageState BROKER_BUSY = new MessageState(-100);
    public static final MessageState CONSUMER_BUSY = new MessageState(-300);
    public static final MessageState CONSUMER_BUSY_NOT_RESEND = new MessageState(-350);
    public static final MessageState CONSUMER_ERROR = new MessageState(-400);
    public static final MessageState CONSUMER_ERROR_DELAY_RETRY = new MessageState(-420);
    public static final MessageState CONSUMER_ERROR_NOT_RESEND = new MessageState(-450);
    public static final MessageState BROKER_RETRY_FAILED = new MessageState(-500);
    public static final MessageState CONSUMER_NOT_FOUND = new MessageState(-600);
    public static final MessageState CONSUMER_NOT_FOUND_NOT_RESEND = new MessageState(-650);
    public static final MessageState MESSAGE_EXPIRED = new MessageState(-700);
    public static final MessageState MESSAGE_BLOCKED = new MessageState(-800);
    public static final MessageState CONSUMER_GROUP_OFFLINE = new MessageState(-900);
    public static final MessageState CONSUMER_DEAD = new MessageState(-1000);


    private int stateCode;
    
    public MessageState() {
        super();
    }

    private MessageState(int stateCode) {
        this.stateCode = stateCode;
        MAP.put(stateCode, this);
    }

    public int getCode() {
        return stateCode;
    }

    public static MessageState valueOf(int code) {
        return MAP.get(code);
    }
}
