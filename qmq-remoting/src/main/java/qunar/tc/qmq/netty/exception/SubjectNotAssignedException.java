package qunar.tc.qmq.netty.exception;

import qunar.tc.qmq.service.exceptions.MessageException;

/**
 * @author yiqun.fan create on 17-9-8.
 */
public class SubjectNotAssignedException extends MessageException {

    public SubjectNotAssignedException(String messageId) {
        super(messageId, "subject not assigned");
    }

    @Override
    public boolean isSubjectNotAssigned() {
        return true;
    }
}
