package qunar.tc.qmq.consumer.exception;

/**
 * User: zhaohuiyu
 * Date: 10/15/13
 * Time: 1:53 PM
 */
public class DuplicateListenerException extends RuntimeException {
    private static final long serialVersionUID = 1377475808662809865L;
    private String key;

    public DuplicateListenerException(String key) {
        super(key);
        this.key = key;
    }

    public String getKey() {
        return this.key;
    }


}
