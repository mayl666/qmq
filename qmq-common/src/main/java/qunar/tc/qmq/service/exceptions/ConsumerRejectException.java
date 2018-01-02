/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.service.exceptions;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-9
 */
public class ConsumerRejectException extends Exception {

    private static final long serialVersionUID = -7210593505052411928L;

    public ConsumerRejectException(String msg) {
        super(msg);
    }

    @Override
    public Throwable initCause(Throwable cause) {
        return this;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
