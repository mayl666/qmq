/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.service.exceptions;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-6
 */
public class MessageException extends Exception {

    public static final String BROKER_BUSY = "broker busy";

    private static final long serialVersionUID = -8385014158365588186L;

    private final String messageId;

    public MessageException(String messageId, String msg, Throwable t) {
        super(msg, t);
        this.messageId = messageId;
    }

    public MessageException(String messageId, String msg) {
        this(messageId, msg, null);
    }

    public String getMessageId() {
        return messageId;
    }

    @Override
    public Throwable initCause(Throwable cause) {
        return this;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

    public boolean isBrokerBusy() {
        return BROKER_BUSY.equals(getMessage());
    }

    public boolean isSubjectNotAssigned() {
        return false;
    }
}
