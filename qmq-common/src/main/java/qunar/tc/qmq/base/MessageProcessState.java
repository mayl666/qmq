package qunar.tc.qmq.base;

/**
 * User: zhaohuiyu
 * Date: 4/7/13
 * Time: 12:30 PM
 */
public class MessageProcessState {
    public final static Integer COMPLETED = 2;

    public final static Integer PROCESSING = 1;

    public final static Integer FAILED = 0;
    
    public final static Integer UNKNOWN = -1;
}
