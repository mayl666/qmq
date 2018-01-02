/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.service;

import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.ChannelState;
import qunar.tc.qmq.base.QueryRequest;
import qunar.tc.qmq.service.exceptions.ConsumerRejectException;

import java.util.List;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-26
 */
public interface ConsumerMessageHandler {

    ChannelState handle(BaseMessage message) throws ConsumerRejectException;

    List<Integer> queryMessageState(List<QueryRequest> messageIds);
}
