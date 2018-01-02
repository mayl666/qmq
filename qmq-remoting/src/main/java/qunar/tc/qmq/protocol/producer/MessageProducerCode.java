package qunar.tc.qmq.protocol.producer;

/**
 * @author zhenyu.nie created on 2017 2017/7/6 16:04
 */
public class MessageProducerCode {

    public static final int SUCCESS = 0;
    public static final int BROKER_BUSY = 1;
    public static final int MESSAGE_DUPLICATE = 2;
    public static final int SUBJECT_NOT_ASSIGNED = 3;
    public static final int BROKER_READ_ONLY = 4;
    public static final int BLOCK = 5;
    public static final int STORE_ERROR = 6;

}
