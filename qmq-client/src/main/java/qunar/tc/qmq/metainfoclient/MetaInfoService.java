package qunar.tc.qmq.metainfoclient;

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.concurrent.NamedThreadFactory;
import qunar.tc.qmq.base.BrokerGroup;
import qunar.tc.qmq.base.BrokerState;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.common.MapKeyBuilder;
import qunar.tc.qmq.metainfoclient.impl.MetaInfoClientNettyImpl;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.consumer.MetaInfoResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author yiqun.fan create on 17-8-31.
 */
public class MetaInfoService implements MetaInfoClient.ResponseSubscriber, Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaInfoService.class);
    private static final long REFRESH_INTERVAL_SECONDS = 20;

    private static final Supplier<MetaInfoService> SUPPLIER = Suppliers.memoize(new Supplier<MetaInfoService>() {
        @Override
        public MetaInfoService get() {
            MetaInfoService service = new MetaInfoService();
            MetaInfoClientNettyImpl.getClient().registerResponseSubscriber(service);
            return service;
        }
    });
    private final MetaInfoClient client = MetaInfoClientNettyImpl.getClient();
    private final EventBus eventBus = new EventBus("meta-info");
    private final Set<String> metaInfoKeys = Sets.newConcurrentHashSet();

    private MetaInfoService() {
        Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-meta-refresh"))
                .scheduleAtFixedRate(this, REFRESH_INTERVAL_SECONDS, REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public static MetaInfoService getInstance() {
        return SUPPLIER.get();
    }

    @Override
    public void run() {
        refresh();
    }

    private void refresh() {
        for (String key : metaInfoKeys) {
            List<String> strs = MapKeyBuilder.splitKey(key);
            request(ClientType.valueOf(strs.get(0)), strs.get(1));
        }
    }

    public void register(Object subscriber) {
        eventBus.register(subscriber);
    }

    public void request(ClientType clientType, String subject) {
        MetaInfoRequest request = new MetaInfoRequest();
        request.setClientTypeCode(clientType.getCode());
        request.setSubject(subject);
        LOGGER.debug("meta info request: {}", request);
        client.sendRequest(request);
        String key = MapKeyBuilder.buildMetaInfoKey(clientType, subject);
        metaInfoKeys.add(key);
    }

    @Override
    public void onResponse(MetaInfoResponse response) {
        MetaInfo metaInfo = parseResponse(response);
        if (metaInfo != null) {
            if (metaInfo.getClusterInfo().getGroups().isEmpty()) {
                LOGGER.info("meta server return empty broker, will retry in a few seconds. subject={}, client={}",
                        metaInfo.getSubject(), metaInfo.getClientType().name());
            } else {
                LOGGER.debug("meta info: {}", metaInfo);
                eventBus.post(metaInfo);
            }
        } else {
            LOGGER.warn("request meta info fail, will retry in a few seconds.");
        }
    }

    private MetaInfo parseResponse(MetaInfoResponse response) {
        if (response == null) {
            return null;
        }
        String subject = response.getSubject();
        ClientType clientType = parseClientType(response);
        List<BrokerGroup> groups = response.getBrokerCluster() == null ? null : response.getBrokerCluster().getBrokerGroups();
        if (Strings.isNullOrEmpty(subject) || clientType == null) {
            return null;
        }
        if (groups == null || groups.isEmpty()) {
            return new MetaInfo(subject, clientType, new BrokerClusterInfo());
        }
        List<BrokerGroup> filter = new ArrayList<>(groups.size());
        for (BrokerGroup group : groups) {
            if (group == null || Strings.isNullOrEmpty(group.getGroupName()) || Strings.isNullOrEmpty(group.getMaster())) {
                continue;
            }
            BrokerState state = group.getBrokerState();
            if (clientType == ClientType.CONSUMER) {
                if (state == BrokerState.R || state == BrokerState.RW) {
                    filter.add(group);
                }
            } else if (clientType == ClientType.PRODUCER) {
                if (state == BrokerState.W || state == BrokerState.RW) {
                    filter.add(group);
                }
            }
        }
        if (filter.isEmpty()) {
            return new MetaInfo(subject, clientType, new BrokerClusterInfo());
        }
        List<BrokerGroupInfo> groupInfos = new ArrayList<>(filter.size());
        for (int i = 0; i < filter.size(); i++) {
            BrokerGroup bg = filter.get(i);
            groupInfos.add(new BrokerGroupInfo(i, bg.getGroupName(), bg.getMaster(), bg.getSlaves()));
        }
        BrokerClusterInfo clusterInfo = new BrokerClusterInfo(groupInfos);
        return new MetaInfo(subject, clientType, clusterInfo);
    }

    private ClientType parseClientType(MetaInfoResponse response) {
        return ClientType.of(response.getClientTypeCode());
    }
}
