package qunar.tc.qmq.meta.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Conf;
import qunar.tc.qmq.base.BrokerGroup;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.meta.store.Store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public class CachedMetaInfoManager implements Disposable {
    private static final Logger logger = LoggerFactory.getLogger(CachedMetaInfoManager.class);
    private static final ScheduledExecutorService SCHEDULE_POOL = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("meta-info-refresh-%d").build());
    private static final long DEFAULT_REFRESH_PERIOD_SECONDS = 5L;

    private final Store store;
    private final long refreshPeriodSeconds;

    /**
     * groupName -> subjects
     */
    private volatile Map<String, List<String>> cachedGroupSubjects = new HashMap<>();
    /**
     * subject -> groupNames
     */
    private volatile Map<String, List<String>> cachedSubjectGroups = new HashMap<>();
    /**
     * groupName -> brokerGroup
     */
    private volatile Map<String, BrokerGroup> cachedBrokerGroups = new HashMap<>();

    public CachedMetaInfoManager(Store store, Conf conf) {
        this.refreshPeriodSeconds = conf.getLong("refresh.period.seconds", DEFAULT_REFRESH_PERIOD_SECONDS);
        this.store = store;
        refresh();
        initRefreshTask();
    }

    private void initRefreshTask() {
        SCHEDULE_POOL.scheduleAtFixedRate(new RefreshTask(), refreshPeriodSeconds, refreshPeriodSeconds, TimeUnit.SECONDS);
    }

    public List<String> getGroups(String subject) {
        final List<String> groups = cachedSubjectGroups.get(subject);
        if (groups == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(groups);
    }

    public List<String> getSubjects(String groupName) {
        final List<String> subjects = cachedGroupSubjects.get(groupName);
        if (subjects == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(subjects);
    }

    public List<String> getAllBrokerGroupNames() {
        return ImmutableList.copyOf(cachedBrokerGroups.keySet());
    }

    public BrokerGroup getBrokerGroup(String groupName) {
        return cachedBrokerGroups.get(groupName);
    }

    public void executeRefreshTask() {
        try {
            SCHEDULE_POOL.execute(new RefreshTask());
        } catch (Exception e) {
            logger.error("execute refresh task reject");
        }
    }

    private void refresh() {
        refreshBrokerGroups();
        refreshGroupsAndSubjects();
    }

    private void refreshGroupsAndSubjects() {
        final List<SubjectRoute> subjectRoutes = store.getAllSubjectRoutes();
        if (subjectRoutes == null || subjectRoutes.size() == 0) {
            return;
        }

        final Map<String, List<String>> groupSubjects = new HashMap<>();
        final Map<String, List<String>> subjectGroups = new HashMap<>();
        for (SubjectRoute subjectRoute : subjectRoutes) {
            final String subject = subjectRoute.getSubject();
            final List<String> aliveGroupName = new ArrayList<>();

            for (String groupName : subjectRoute.getBrokerGroups()) {
                if (cachedBrokerGroups.containsKey(groupName)) {
                    List<String> value = groupSubjects.computeIfAbsent(groupName, k -> new ArrayList<>());
                    value.add(subject);
                    aliveGroupName.add(groupName);
                }
            }
            subjectGroups.put(subject, aliveGroupName);
        }

        cachedGroupSubjects = groupSubjects;
        cachedSubjectGroups = subjectGroups;
    }

    private void refreshBrokerGroups() {
        final List<BrokerGroup> brokerGroupList = store.getAllBrokerGroups();
        if (brokerGroupList == null || brokerGroupList.size() == 0) {
            return;
        }
        final Map<String, BrokerGroup> brokerGroups = new HashMap<>();
        for (BrokerGroup brokerGroup : brokerGroupList) {
            brokerGroups.put(brokerGroup.getGroupName(), brokerGroup);
        }
        cachedBrokerGroups = brokerGroups;
    }

    @Override
    public void destroy() {
        if (SCHEDULE_POOL != null) {
            SCHEDULE_POOL.shutdown();
        }
    }

    private class RefreshTask implements Runnable {

        @Override
        public void run() {
            try {
                refresh();
            } catch (Exception e) {
                logger.error("refresh failed", e);
            }
        }
    }
}
