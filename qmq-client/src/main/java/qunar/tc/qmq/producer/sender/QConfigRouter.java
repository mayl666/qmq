package qunar.tc.qmq.producer.sender;

import com.google.common.collect.Maps;
import qunar.tc.qconfig.client.Configuration;
import qunar.tc.qconfig.client.Feature;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.ReliabilityLevel;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.producer.RegistryResolver;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;

import static qunar.tc.qmq.config.QConfigConstant.*;


/**
 * User: yee.wang
 * Date: 9/15/14
 * Time: 5:32 PM
 */
public class QConfigRouter implements Router {
    private static final long TREAT_AS_REAL_TIME = 10;
    private static final String REALTIME = "REALTIME";
    private static final String UNRELIABILITY = "UNRELIABILITY";
    private static final String DELAY = "DELAY";

    private final ConcurrentMap<String, Route> dataCenter2Route = Maps.newConcurrentMap();
    private volatile Map<String, String> subject2DataCenter = Collections.emptyMap();

    private final String localDataCenter;
    private final RegistryResolver registryResolver;

    private static final ThreadPoolExecutor PRE_INIT_CONNECTION = new ThreadPoolExecutor(1, 1, 10, TimeUnit.MINUTES, new LinkedBlockingDeque<Runnable>(), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "qmq-client-pre-init-connection");
        }
    });

    public QConfigRouter(RegistryResolver registryResolver) {
        this.registryResolver = registryResolver;
        this.localDataCenter = registryResolver.dataCenter();

        init();
        MapConfig routeConfig = MapConfig.get(QMQ_CLINET_GROUP, ROUTER_ZK, Feature.DEFAULT);
        routeConfig.addListener(new Configuration.ConfigListener<Map<String, String>>() {
            @Override
            public void onLoad(Map<String, String> conf) {
                subject2DataCenter = conf;
            }
        });
    }

    private void init() {
        MapConfig config = MapConfig.get(QMQ_CLINET_GROUP, BROKER_CONFIG, Feature.DEFAULT);
        Map<String, String> map = config.asMap();
        parse(map);

        config.addListener(new Configuration.ConfigListener<Map<String, String>>() {
            @Override
            public void onLoad(Map<String, String> conf) {
                parse(conf);
            }
        });
    }

    private void parse(Map<String, String> conf) {
        for (Map.Entry<String, String> entry : conf.entrySet()) {
            String[] arr = entry.getValue().split(SPLITER);
            String registry = registryResolver.resolve(arr[0]);
            dataCenter2Route.put(entry.getKey(), new DubboRoute(registry, arr[1]));
        }
    }

    @Override
    public Connection route(Message message) {
        Route route = doRoute(message);
        return route.route();
    }

    private Route doRoute(Message message) {
        final String dataCenter = subject2DataCenter.get(message.getSubject());
        final Route localRouter = route(type(message), localDataCenter);

        if (dataCenter == null) {
            return localRouter;
        }

        final Route route = route(dataCenter);
        if (route == Router.NOROUTE) {
            return localRouter;
        }

        if (route.isNewRoute()) {
            preHeat(route);
            return localRouter;
        }
        return route;
    }

    private Route route(String type, String dataCenter) {
        final Route route = dataCenter2Route.get(type + '-' + dataCenter);
        return route != null ? route : route(type);
    }

    private Route route(String dataCenter) {
        final Route route = dataCenter2Route.get(dataCenter);
        return route != null ? route : Router.NOROUTE;
    }

    private String type(Message message) {
        if (ReliabilityLevel.isLow(message)) {
            return UNRELIABILITY;
        } else {
            Object scheduleReceiveTimeProperty = message.getProperty(BaseMessage.keys.qmq_scheduleRecevieTime.name());
            if (scheduleReceiveTimeProperty == null) return REALTIME;

            if (((Long) scheduleReceiveTimeProperty - System.currentTimeMillis()) > TREAT_AS_REAL_TIME) {
                return DELAY;
            } else {
                return REALTIME;
            }
        }
    }

    private void preHeat(final Route route) {
        PRE_INIT_CONNECTION.execute(new Runnable() {
            @Override
            public void run() {
                route.preHeat();
            }
        });
    }
}
