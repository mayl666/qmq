package qunar.tc.qmq.consumer.register;

import com.alibaba.dubbo.config.ReferenceConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qconfig.client.Configuration;
import qunar.tc.qconfig.client.Feature;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.base.SubscribeRequest;
import qunar.tc.qmq.service.ConsumerMessageHandler;
import qunar.tc.qmq.service.SubscribeService;
import qunar.tc.qmq.utils.ReferenceBuilder;
import qunar.tc.qmq.utils.URLUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.dubbo.common.Constants.CLUSTER_KEY;
import static qunar.tc.qmq.config.QConfigConstant.DIRECT_CONSUMER_SWITCH;
import static qunar.tc.qmq.config.QConfigConstant.QMQ_CLINET_GROUP;
import static qunar.tc.qmq.utils.Constants.BROKER_GROUP_ROOT;
import static qunar.tc.qmq.utils.Constants.SUBSCRIBE_GROUP;
import static qunar.tc.qtracer.Constants.DUBBO_TRACE_SWITCH_KEY;


/**
 * @author yunfeng.yang
 * @since 2017/1/16
 */
class DirectConsumerRegister implements ConsumerRegister, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(DirectConsumerRegister.class);

    private static final int HEARTBEAT_INTERVAL_MINUTES = 30;
    private static final int HEARTBEAT_INITIAL_DELAY_MINUTES = 30;
    private static final ScheduledExecutorService POOL = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("consumer-heartbeat-%d").build());

    private static final Object HOLDER = new Object();

    private final ReferenceConfig<SubscribeService> agent;
    private final ConsumerMessageHandler handler;
    private final Map<SubscribeRequest, Object> subscribers = new ConcurrentHashMap<>();

    private volatile boolean directConsumerSwitch = false;

    private String brokerGroupRoot = BROKER_GROUP_ROOT;
    private SubscribeInfo info;

    DirectConsumerRegister(final String brokerGroupRoot, final SubscribeInfo info, final ConsumerMessageHandler handler) {
        Preconditions.checkNotNull(handler, "消息处理handler不能为null");

        this.info = info;
        this.handler = handler;
        if (!Strings.isNullOrEmpty(brokerGroupRoot)) {
            this.brokerGroupRoot = brokerGroupRoot;
        }

        initSwitchListener(info);
        this.agent = initSubscribeAgent(info.getRegistry());
        POOL.scheduleAtFixedRate(this, HEARTBEAT_INITIAL_DELAY_MINUTES, HEARTBEAT_INTERVAL_MINUTES, TimeUnit.MINUTES);
    }

    private void initSwitchListener(final SubscribeInfo info) {
        MapConfig switchConfig = MapConfig.get(QMQ_CLINET_GROUP, DIRECT_CONSUMER_SWITCH, Feature.DEFAULT);
        switchConfig.asMap();
        switchConfig.addListener(new Configuration.ConfigListener<Map<String, String>>() {
            @Override
            public void onLoad(Map<String, String> conf) {
                try {
                    String s = conf.get(info.getAppCode());
                    if (Strings.isNullOrEmpty(s)) {
                        s = conf.get("switch");
                    }

                    boolean origin = directConsumerSwitch;
                    directConsumerSwitch = Boolean.valueOf(s);

                    autoSwitch(origin);
                } catch (Throwable t) {
                    logger.error("获取直连consumer开关失败, appCode:{}", info.getAppCode(), t);
                }
            }
        });
    }

    private void autoSwitch(boolean origin) {
        if (origin == directConsumerSwitch) {
            return;
        }

        if (directConsumerSwitch) {
            subscribeAll();
        } else {
            unsubscribeAll();
        }
    }

    private ReferenceConfig<SubscribeService> initSubscribeAgent(String registry) {
        ReferenceConfig<SubscribeService> ref = null;
        try {
            final String subscribeGroup = String.format("%s/%s", brokerGroupRoot, SUBSCRIBE_GROUP);
            ref = ReferenceBuilder
                    .newRef(SubscribeService.class)
                    .withRegistryAddress(URLUtils.buildZKUrl(registry, subscribeGroup))
                    .build();
            ref.setCheck(Boolean.FALSE);
            Map<String, String> parameters = new HashMap<>();
            parameters.put(DUBBO_TRACE_SWITCH_KEY, "false");
            parameters.put(CLUSTER_KEY, "broadcast");
            ref.setParameters(parameters);
            ref.get();
        } catch (Throwable t) {
            logger.debug("创建consumer订阅服务失败,registry:", registry, t);
        }
        return ref;
    }

    @Override
    public void regist(String prefix, String group, RegistParam param) {
        try {
            SubscribeRequest request = buildSubscribeRequest(prefix, group, param.getExecutorConfig());
            subscribers.put(request, HOLDER);

            if (!directConsumerSwitch) {
                return;
            }
            info.addExecutorConfig(prefix + "-" + group, param.getExecutorConfig());
            Set<SubscribeRequest> requests = new HashSet<>();
            requests.add(request);

            agent.get().subscribe(requests, handler);
            logger.info("prefix[{}], group[{}]订阅消息成功, registry:{}", prefix, group, info.getRegistry());
        } catch (Throwable t) {
            logger.info("prefix[{}], group[{}]订阅消息失败, registry:{}", prefix, group, info.getRegistry(), t);
        }
    }

    @Override
    public void unregist(String prefix, String group) {
        try {
            SubscribeRequest request = buildSubscribeRequest(prefix, group, info.getExecutorConfig(prefix + "-" + group));
            subscribers.remove(request);

            if (!directConsumerSwitch) {
                return;
            }
            Set<SubscribeRequest> requests = new HashSet<>();
            requests.add(request);
            agent.get().unsubscribe(requests);
            logger.info("prefix[{}], group[{}]取消订阅消息成功, registry:{}", prefix, group, info.getRegistry());
        } catch (Throwable t) {
            logger.info("prefix[{}], group[{}]取消订阅消息失败, registry:{}", prefix, group, info.getRegistry(), t);
        }
    }

    private SubscribeRequest buildSubscribeRequest(String prefix, String group, ExecutorConfig config) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prefix), "prefix can not null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group), "group can not null");
        return new SubscribeRequest(info.buildConsumerUrl(prefix, config), prefix, group);
    }

    @Override
    public String registry() {
        return info.getRegistry();
    }

    @Override
    public boolean offline() {
        return true;
    }

    @Override
    public boolean online() {
        return true;
    }

    public void setAutoOnline(boolean autoOnline) {
    }

    public void destroy() {
        agent.destroy();
    }

    @Override
    public void run() {
        if (directConsumerSwitch) {
            subscribeAll();
        }
    }

    private void unsubscribeAll() {
        try {
            if (subscribers.size() != 0) {
                Set<SubscribeRequest> requests = subscribers.keySet();
                agent.get().unsubscribe(requests);
            }
            logger.debug("consumer取消订阅消息成功");
        } catch (Throwable t) {
            logger.error("consumer取消订阅失败，registry:{}", info.getRegistry(), t);
        }
    }

    private void subscribeAll() {
        try {
            if (subscribers.size() != 0) {
                Set<SubscribeRequest> requests = subscribers.keySet();
                agent.get().subscribe(requests, handler);
            }
            logger.debug("consumer订阅消息成功");
        } catch (Throwable t) {
            logger.error("consumer订阅失败，registry:{}", info.getRegistry(), t);
        }
    }
}
