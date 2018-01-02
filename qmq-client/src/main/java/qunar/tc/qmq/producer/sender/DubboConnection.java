package qunar.tc.qmq.producer.sender;

import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.rpc.RpcException;
import com.google.common.collect.Lists;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.netty.exception.RemoteTimeoutException;
import qunar.tc.qmq.service.BrokerMessageService;
import qunar.tc.qmq.service.exceptions.MessageException;
import qunar.tc.qmq.utils.ReferenceBuilder;

import java.util.List;
import java.util.Map;

/**
 * @author zhenyu.nie created on 2017 2017/7/3 12:30
 */
class DubboConnection implements Connection {

    private final ReferenceConfig<BrokerMessageService> service;

    private String url;

    public DubboConnection(String registry) {
        this.url = registry;
        this.service = ReferenceBuilder
                .newRef(BrokerMessageService.class)
                .withRegistryAddress(registry)
                .build();
    }

    @Override
    public void preHeat() {
        service.get();
    }

    @Override
    public String url() {
        return url;
    }

    @Override
    public Map<String, MessageException> send(List<ProduceMessage> messages) throws RemoteTimeoutException {
        List<BaseMessage> baseMessages = Lists.newArrayListWithCapacity(messages.size());
        for (ProduceMessage produceMessage : messages) {
            baseMessages.add((BaseMessage) produceMessage.getBase());
        }
        try {
            return service.get().send(baseMessages);
        } catch (RpcException e) {
            if (e.isTimeout()) {
                throw new RemoteTimeoutException(e);
            }
            throw e;
        }
    }

    @Override
    public void destroy() {
        service.destroy();
    }
}
