package qunar.tc.qmq.metainfoclient;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.concurrent.NamedThreadFactory;
import qunar.tc.qconfig.client.Configuration;
import qunar.tc.qconfig.client.Feature;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.common.SplitterUtil;
import qunar.tc.qmq.config.QConfigConstant;
import qunar.tc.qmq.netty.client.HttpClient;
import qunar.tc.qmq.netty.client.HttpResponseCallback;
import qunar.tc.qmq.netty.client.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yiqun.fan create on 17-8-30.
 */
public class MetaServerAddressHelper implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaServerAddressHelper.class);
    private static final int DELAY_RETRY_NUM = 5;
    private static final long DELAY_TIME_MS = 5000;
    private static final long REFRESH_INTERVAL_SECONDS = 60;
    private static final MetaServerAddressHelper helper = new MetaServerAddressHelper();

    public static String refreshAndGetAddress() {
        helper.refresh();
        return getAddress();
    }

    public static String getAddress() {
        try {
            return helper.getAddr();
        } catch (Exception e) {
            LOGGER.error("get meta server address exception", e);
            return "";
        }
    }

    private final HttpClient httpClient = HttpClient.newClient();
    private volatile Future<List<String>> current;
    private final ReentrantLock requestLock = new ReentrantLock();
    private final AtomicReference<String> metaIpServerlink = new AtomicReference<>();
    private final AtomicReference<List<String>> metaServerAddrs = new AtomicReference<>((List<String>) (new ArrayList<String>()));

    private MetaServerAddressHelper() {
        loadIpServerConfig();
        init();
    }

    private void loadIpServerConfig() {
        MapConfig config = MapConfig.get(QConfigConstant.QMQ_CLINET_GROUP, QConfigConstant.META_IPSERVER_CONFIG, Feature.DEFAULT);
        config.asMap();
        config.addListener(new Configuration.ConfigListener<Map<String, String>>() {
            @Override
            public void onLoad(Map<String, String> conf) {
                metaIpServerlink.set(conf.get("url"));
            }
        });
        LOGGER.info("meta ip server: {}", metaIpServerlink.get());
    }

    private void init() {
        int retryNum = 0;
        while (!refresh()) {
            long sleep = 500;
            if (retryNum++ >= DELAY_RETRY_NUM) {
                sleep = DELAY_TIME_MS;
            }
            LOGGER.info("retry request meta ip server after {}ms", sleep);
            try {
                Thread.sleep(sleep);
            } catch (Exception e) {
                //
            }
        }
        Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("qmq-metaip-refresh"))
                .scheduleAtFixedRate(this, REFRESH_INTERVAL_SECONDS, REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        refresh();
    }

    private boolean refresh() {
        try {
            List<String> addrList = request().get();
            if (addrList == null || addrList.isEmpty()) {
                LOGGER.warn("meta ip server return empty. metaIpServer address: {}", metaIpServerlink.get());
                return false;
            }
            List<String> oldAddrList = metaServerAddrs.get();
            if (!isEqualList(addrList, oldAddrList)) {
                metaServerAddrs.set(addrList);
                LOGGER.info("meta server address: {}", metaServerAddrs.get());
            }
            return true;
        } catch (Exception e) {
            LOGGER.warn("get meta server address exception", e);
            return false;
        }
    }

    private Future<List<String>> request() {
        requestLock.lock();
        try {
            if (current == null) {
                current = httpClient.get(metaIpServerlink.get(), null, new HttpResponseCallback<List<String>>() {
                    @Override
                    public List<String> onCompleted(Response response) throws Exception {
                        return SplitterUtil.COMMA_SPLITTER.splitToList(response.getBody());
                    }

                    @Override
                    public void onThrowable(Throwable t) {
                        LOGGER.error("request meta ip server exception", t);
                    }
                });
            }
            return current;
        } finally {
            requestLock.unlock();
        }
    }

    private boolean isEqualList(List<String> l1, List<String> l2) {
        if (l1.size() != l2.size()) {
            return false;
        }
        for (int i = 0; i < l1.size(); i++) {
            if (!Objects.equal(l1.get(i), l2.get(i))) {
                return false;
            }
        }
        return true;
    }

    private String getAddr() {
        List<String> addrs = metaServerAddrs.get();
        if (addrs.size() == 1) {
            return addrs.get(0);
        }
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        return addrs.get(random.nextInt(addrs.size()));
    }
}
