package qunar.tc.qmq.producer.sender;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import qunar.tc.qconfig.client.Configuration;
import qunar.tc.qconfig.client.Feature;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.config.QConfigConstant;
import qunar.tc.qmq.producer.QueueSender;

import java.util.Map;

/**
 * @author zhenyu.nie created on 2017 2017/7/3 14:15
 */
public class DefaultRouterManagerDispatcher implements RouterManagerDispatcher {

    private volatile DispatchInfo dispatchInfo;

    private final Map<String, RouterManager> routerManagers;

    public DefaultRouterManagerDispatcher(RouterManager routerManager) {
        ImmutableMap.Builder<String, RouterManager> builder = new ImmutableMap.Builder<>();
        RouterManager newQmqRouterManager = new NettyRouterManager();
        builder.put(newQmqRouterManager.name(), newQmqRouterManager);
        builder.put(routerManager.name(), routerManager);
        routerManagers = builder.build();
        doInit(routerManagers);
    }

    private void doInit(final Map<String, RouterManager> routerManagers) {
        // todo: 以后信息从这里挪出去，并且使用url，加上额外参数
        MapConfig mapConfig = MapConfig.get(QConfigConstant.QMQ_CLINET_GROUP, QConfigConstant.ROUTER_DISPATCH, Feature.DEFAULT);
        mapConfig.asMap();
        mapConfig.addListener(new Configuration.ConfigListener<Map<String, String>>() {
            @Override
            public void onLoad(Map<String, String> conf) {
                RouterManager defaultManager = routerManagers.get(conf.get(QConfigConstant.DEFAULT));
                if (defaultManager == null) {
                    defaultManager = NOP_ROUTER_MANAGER;
                }
                ImmutableMap.Builder<String, RouterManager> builder = new ImmutableMap.Builder<String, RouterManager>();
                for (Map.Entry<String, String> entry : conf.entrySet()) {
                    String subject = entry.getKey().trim();
                    String type = entry.getValue().trim();
                    if (!subject.equals(QConfigConstant.DEFAULT) && !Strings.isNullOrEmpty(type)) {
                        RouterManager routerManager = routerManagers.get(type);
                        if (routerManager == null) {
                            routerManager = NOP_ROUTER_MANAGER;
                        }
                        builder.put(subject, routerManager);
                    }
                }
                dispatchInfo = new DispatchInfo(defaultManager, builder.build());
            }
        });
        Preconditions.checkNotNull(dispatchInfo);
    }

    @Override
    public void init() {
        for (RouterManager routerManager : routerManagers.values()) {
            routerManager.init();
        }
    }

    @Override
    public RouterManager dispatch(Message message) {
        return dispatchInfo.dispatch(message.getSubject());
    }

    @Override
    public void destroy() {
        for (RouterManager routerManager : routerManagers.values()) {
            routerManager.destroy();
        }
    }

    private static final QueueSender NOP_SENDER = new QueueSender() {
        @Override
        public boolean offer(ProduceMessage pm) {
            return true;
        }

        @Override
        public boolean offer(ProduceMessage pm, long millisecondWait) {
            return true;
        }

        @Override
        public void send(ProduceMessage pm) {

        }

        @Override
        public void destroy() {

        }
    };

    private static final RouterManager NOP_ROUTER_MANAGER = new RouterManager() {
        @Override
        public void init() {

        }

        @Override
        public String name() {
            return "nop";
        }

        @Override
        public String registryOf(Message message) {
            return "";
        }

        @Override
        public Connection routeOf(Message message) {
            return NopRoute.NOP_CONNECTION;
        }

        @Override
        public QueueSender getSender() {
            return NOP_SENDER;
        }

        @Override
        public void destroy() {

        }
    };

    private static class DispatchInfo {

        private final RouterManager defaultRouterManager;

        private final Map<String, RouterManager> subjectRouterManagerMapping;

        public DispatchInfo(RouterManager defaultRouterManager, Map<String, RouterManager> subjectRouterManagerMapping) {
            this.defaultRouterManager = defaultRouterManager;
            this.subjectRouterManagerMapping = subjectRouterManagerMapping;
        }

        public RouterManager dispatch(String subject) {
            RouterManager routerManager = subjectRouterManagerMapping.get(subject);
            if (routerManager != null) {
                return routerManager;
            } else {
                return defaultRouterManager;
            }
        }
    }
}
