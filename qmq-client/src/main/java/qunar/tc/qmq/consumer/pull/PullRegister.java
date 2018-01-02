package qunar.tc.qmq.consumer.pull;

import qunar.concurrent.NamedThreadFactory;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.MapKeyBuilder;
import qunar.tc.qmq.config.NettyClientConfigManager;
import qunar.tc.qmq.consumer.register.ConsumerRegister;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author yiqun.fan create on 17-8-17.
 */
public class PullRegister implements ConsumerRegister {
    private volatile Boolean isOnline = false;
    private final BrokerService brokerService = BrokerServiceImpl.getInstance();
    private final Map<String, PullEntry> pullEntryMap = new HashMap<>();
    private final ExecutorService pullExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("qmq-pull"));
    private final NettyClient nettyClient = NettyClient.getClient();

    @Override
    public synchronized void regist(String subject, String group, RegistParam param) {
        registPullEntry(subject, group, param);
        registPullEntry(RetrySubjectUtils.buildRetrySubject(subject, group), group, param);
    }

    private void registPullEntry(String subject, String group, RegistParam param) {
        final String subcribeKey = MapKeyBuilder.buildSubscribeKey(subject, group);
        PullEntry pullEntry = pullEntryMap.get(subcribeKey);
        if (pullEntry == null) {
            pullEntry = createAndSubmitPullEntry(subject, group, param);
        }
        if (isOnline) {
            pullEntry.online();
        } else {
            pullEntry.offline();
        }
    }

    private PullEntry createAndSubmitPullEntry(String subject, String group, RegistParam param) {
        PushConsumer pushConsumer = new PushConsumer(subject, group, param.getMessageListener(), param.getExecutor());
        PullEntry pullEntry = new PullEntry(brokerService, pushConsumer, param.isBroadcast());
        pullEntryMap.put(MapKeyBuilder.buildSubscribeKey(subject, group), pullEntry);
        pullExecutor.submit(pullEntry);
        return pullEntry;
    }

    @Override
    public synchronized void unregist(String subject, String group) {
        offline(subject, group);
        offline(RetrySubjectUtils.buildRetrySubject(subject, group), group);
    }

    private void offline(String subject, String group) {
        final PullEntry pullEntry = pullEntryMap.get(MapKeyBuilder.buildSubscribeKey(subject, group));
        if (pullEntry != null) {
            pullEntry.offline();
        }
    }

    @Override
    public String registry() {
        return null;
    }

    @Override
    public synchronized void setAutoOnline(boolean autoOnline) {
        if (autoOnline) {
            online();
        } else {
            offline();
        }
        isOnline = autoOnline;
    }

    @Override
    public synchronized void destroy() {
        for (PullEntry pullEntry : pullEntryMap.values()) {
            if (pullEntry != null) {
                pullEntry.destory();
            }
        }
    }

    @Override
    public synchronized boolean offline() {
        isOnline = false;
        for (PullEntry pullEntry : pullEntryMap.values()) {
            if (pullEntry != null) {
                pullEntry.offline();
            }
        }
        return true;
    }

    @Override
    public synchronized boolean online() {
        isOnline = true;
        nettyClient.start(NettyClientConfigManager.get().getDefaultClientConfig());
        for (PullEntry pullEntry : pullEntryMap.values()) {
            if (pullEntry != null) {
                pullEntry.online();
            }
        }
        return true;
    }
}
