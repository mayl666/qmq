package qunar.tc.qmq.broker.impl;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.eventbus.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.MapKeyBuilder;
import qunar.tc.qmq.metainfoclient.MetaInfo;
import qunar.tc.qmq.metainfoclient.MetaInfoService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BrokerServiceImpl implements BrokerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServiceImpl.class);

    private static final Supplier<BrokerServiceImpl> SUPPLIER = Suppliers.memoize(new Supplier<BrokerServiceImpl>() {
        @Override
        public BrokerServiceImpl get() {
            BrokerServiceImpl brokerService = new BrokerServiceImpl();
            MetaInfoService.getInstance().register(brokerService);
            return brokerService;
        }
    });

    public static BrokerService getInstance() {
        return SUPPLIER.get();
    }

    private BrokerServiceImpl() {
    }

    private final ConcurrentMap<String, ClusterFuture> clusterMap = new ConcurrentHashMap<>();
    private final MetaInfoService metaInfoService = MetaInfoService.getInstance();

    @Subscribe
    public void onReceiveMetaInfo(MetaInfo metaInfo) {
        String key = MapKeyBuilder.buildMetaInfoKey(metaInfo.getClientType(), metaInfo.getSubject());
        ClusterFuture future = clusterMap.get(key);
        if (future == null) {
            future = new ClusterFuture(metaInfo.getClusterInfo());
            ClusterFuture oldFuture = clusterMap.putIfAbsent(key, future);
            if (oldFuture != null) {
                oldFuture.set(metaInfo.getClusterInfo());
            }
        } else {
            future.set(metaInfo.getClusterInfo());
        }
    }

    @Override
    public BrokerClusterInfo getClusterBySubject(ClientType clientType, String subject) {
        String key = MapKeyBuilder.buildMetaInfoKey(clientType, subject);
        ClusterFuture future = clusterMap.get(key);
        if (future == null) {
            future = request(clientType, subject, false);
        }
        return future.get();
    }

    @Override
    public void refresh(ClientType clientType, String subject) {
        request(clientType, subject, true);
    }

    private ClusterFuture request(ClientType clientType, String subject, boolean refresh) {
        String key = MapKeyBuilder.buildMetaInfoKey(clientType, subject);
        ClusterFuture newFuture = new ClusterFuture();
        ClusterFuture oldFuture = clusterMap.putIfAbsent(key, newFuture);
        if (oldFuture != null) {
            if (refresh && !oldFuture.inRequest.get()) {
                oldFuture.inRequest.set(true);
                metaInfoService.request(clientType, subject);
            }
            return oldFuture;
        }
        metaInfoService.request(clientType, subject);
        return newFuture;
    }

    private static final class ClusterFuture {
        private final CountDownLatch latch;
        private final AtomicReference<BrokerClusterInfo> cluster;
        private final AtomicBoolean inRequest;

        ClusterFuture() {
            latch = new CountDownLatch(1);
            cluster = new AtomicReference<>(null);
            inRequest = new AtomicBoolean(true);
        }

        ClusterFuture(BrokerClusterInfo cluster) {
            latch = new CountDownLatch(0);
            this.cluster = new AtomicReference<>(cluster);
            inRequest = new AtomicBoolean(false);
        }

        void set(BrokerClusterInfo cluster) {
            this.cluster.set(cluster);
            latch.countDown();
            inRequest.set(false);
        }

        public BrokerClusterInfo get() {
            while (true) {
                try {
                    latch.await();
                    break;
                } catch (Exception e) {
                    LOGGER.warn("get broker cluster info be interrupted, and ignore");
                }
            }
            return cluster.get();
        }
    }
}
