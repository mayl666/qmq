/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.base;

import com.google.common.base.Strings;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-28
 */
public class ACKMessage implements Serializable {
    private static final long serialVersionUID = -3842115341446885752L;

    private static final long UNSET = -1;

    private final String messageID;
    private final String prefix;
    private final String group;
    private final long elapsed;
    private final String errorMessage;
    private long next = UNSET;

    public ACKMessage(String messageID, String prefix, String group, long elapsed, String errorMessage) {
        this.messageID = messageID;
        this.prefix = prefix;
        this.group = group;
        this.elapsed = elapsed;
        this.errorMessage = errorMessage;
    }

    public boolean isFailed() {
        return !Strings.isNullOrEmpty(errorMessage);
    }

    public String getMessageID() {
        return messageID;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getGroup() {
        return group;
    }

    public long getElapsed() {
        return elapsed;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setNext(long next) {
        this.next = next;
    }

    public long getNext() {
        return this.next;
    }

    @Override
    public String toString() {
        return "ACKMessage [messageID=" + messageID + ", prefix=" + prefix + ", group=" + group + ", elapsed="
                + elapsed + ", errorMessage=" + errorMessage + "]";
    }
}
