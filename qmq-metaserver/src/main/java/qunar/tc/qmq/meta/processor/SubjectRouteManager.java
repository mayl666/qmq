package qunar.tc.qmq.meta.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Conf;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.base.BrokerGroup;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.loadbalance.LoadBalance;
import qunar.tc.qmq.meta.loadbalance.RandomLoadBalance;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.meta.monitor.QMon;
import qunar.tc.qmq.meta.store.Store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author yunfeng.yang
 * @since 2017/8/29
 */
class SubjectRouteManager {
    private static final Logger logger = LoggerFactory.getLogger(SubjectRouteManager.class);
    private static final int DEFAULT_MIN_NUM = 2;

    private final CachedMetaInfoManager cachedMetaInfoManager;
    private final Store store;
    private final LoadBalance loadBalance;
    private int minGroupNum = DEFAULT_MIN_NUM;

    SubjectRouteManager(CachedMetaInfoManager cachedMetaInfoManager, Store store) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
        this.store = store;
        this.loadBalance = new RandomLoadBalance();

        MapConfig.get("subject_route.properties").addListener(conf -> {
            Conf confMap = Conf.fromMap(conf);
            minGroupNum = confMap.getInt("min.group.num", DEFAULT_MIN_NUM);
        });
    }

    List<BrokerGroup> route(String subject, int clientTypeCode) {
        final List<String> cachedGroupNames = cachedMetaInfoManager.getGroups(subject);
        if (cachedGroupNames != null && cachedGroupNames.size() != 0) {
            return selectExistedBrokerGroups(cachedGroupNames);
        }

        if (clientTypeCode == ClientType.CONSUMER.getCode()) {
            return Collections.emptyList();
        }

        final List<String> brokerGroupNames = cachedMetaInfoManager.getAllBrokerGroupNames();
        if (brokerGroupNames == null || brokerGroupNames.size() == 0) {
            logger.error("find no broker groups");
            return Collections.emptyList();
        }

        final List<String> newRoute = insertOrSelectFromStore(subject, brokerGroupNames);
        // 刷新下本地缓存
        cachedMetaInfoManager.executeRefreshTask();
        return selectExistedBrokerGroups(newRoute);
    }

    private List<String> insertOrSelectFromStore(String subject, List<String> brokerGroupNames) {
        List<String> newRoute = loadBalance.select(subject, brokerGroupNames, minGroupNum);
        final int num = store.insertSubjectRoute(subject, newRoute);
        if (num == 0) {
            final SubjectRoute subjectRoute = store.selectSubjectRoute(subject);
            newRoute = subjectRoute.getBrokerGroups();
        }
        QMon.clientSubjectRouteCountInc(subject);
        return newRoute;
    }

    private List<BrokerGroup> selectExistedBrokerGroups(List<String> cachedGroupNames) {
        final List<BrokerGroup> cachedBrokerGroups = new ArrayList<>();
        for (String groupName : cachedGroupNames) {
            final BrokerGroup brokerGroup = cachedMetaInfoManager.getBrokerGroup(groupName);
            if (brokerGroup != null) {
                cachedBrokerGroups.add(brokerGroup);
            }
        }
        return cachedBrokerGroups;
    }
}
