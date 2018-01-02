package qunar.tc.qmq.consumer.register;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.concurrent.ManagedExecutors;
import qunar.tc.common.zookeeper.ConnectionState;
import qunar.tc.common.zookeeper.ConnectionStateListener;
import qunar.tc.common.zookeeper.ZKClient;
import qunar.tc.common.zookeeper.ZKClientCache;

import javax.annotation.PreDestroy;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Created by IntelliJ IDEA.
 * User: liuzz
 * Date: 12-12-28
 * Time: 下午2:40
 */
class ZKConsumerRegister implements ConsumerRegister {
    private static final Logger log = LoggerFactory.getLogger(ZKConsumerRegister.class);

    private static final int RETRY_TIME = 3;

    private static final int RECOVER_TIME = 60;

    private final ZKClient zkClient;

    private volatile boolean isOnline = false;

    private static final Object HOLDER = new Object();

    private final Map<Path, Object> subscribers = new ConcurrentHashMap<>();

    private final Map<Path, Object> registedUrls = new ConcurrentHashMap<>();

    private final ConcurrentMap<Path, Action> failedActions = new ConcurrentHashMap<>();
    private final BlockingQueue<Action> actions = new LinkedBlockingDeque<>();

    private SubscribeInfo info;

    ZKConsumerRegister(SubscribeInfo info) {
        this.info = info;
        this.zkClient = initZK(info.getRegistry());
        initRecover();
    }

    /**
     * 注册的时候并不立即注册到zk
     *
     * @param prefix   订阅的前缀
     * @param group    消费组
     * @param executor 订阅时线程池配置
     */
    @Override
    public void regist(String prefix, String group, RegistParam param) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prefix), "prefix can not null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(group), "group can not null");
        String parentPath = info.buildParentPath(prefix, group);
        String finalPath = info.buildFinalPath(prefix, parentPath, param.getExecutorConfig());
        Path path = new Path(parentPath, finalPath);
        info.addExecutorConfig(prefix + "-" + group, param.getExecutorConfig());//移除时需要使用

        subscribers.put(path, HOLDER);

        //如果已经上线则可以注册到zk
        if (!isOnline) return;

        regist(path);
    }

    /**
     * 取消订阅
     *
     * @param prefix 订阅的前缀
     * @param group  消费组
     */
    @Override
    public void unregist(String prefix, String group) {
        String parentPath = info.buildParentPath(prefix, group);
        String finalPath = info.buildFinalPath(prefix, parentPath, info.getExecutorConfig(prefix + "-" + group));
        Path path = new Path(parentPath, finalPath);
        subscribers.remove(path);
        unregist(path);
    }

    @Override
    public boolean online() {
        if (isOnline) return true;
        Set<Path> copy = new HashSet<>(subscribers.keySet());
        isOnline = true;

        if (copy.size() == 0) return true;

        for (Path p : copy) {
            regist(p);
        }
        return true;
    }

    @Override
    public boolean offline() {
        isOnline = false;
        Set<Path> copy = new HashSet<>(registedUrls.keySet());
        if (copy.size() == 0) return true;

        for (Path p : copy) {
            unregist(p);
        }
        return true;
    }

    private void regist(final Path path) {
        Action remove = failedActions.remove(path);
        if (remove != null) remove.cancel();

        if (!doRegist(path)) {
            AbstractAction failedAction = new AbstractAction(path, true) {
                @Override
                protected void doExec() {
                    regist(path);
                }
            };
            Action old = failedActions.putIfAbsent(path, failedAction);
            if (old == null) actions.add(failedAction);
            return;
        }

        log.info("消费者 {} 上线", path.path);
        registedUrls.put(path, HOLDER);
    }

    private boolean doRegist(Path path) {
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                if (!zkClient.checkExist(path.parent))
                    zkClient.addPersistentNode(path.parent);
            } catch (KeeperException.NodeExistsException e) {
                log.debug("zk node already exist. path: {}", path.parent, e);
            } catch (Throwable e) {
                log.error("registe node is {}, add to fail url to retry", path.parent);
                continue;
            }

            try {
                if (zkClient.checkExist(path.path))
                    zkClient.deletePath(path.path);
            } catch (KeeperException.NoNodeException e) {
                log.debug("zk node not exist. path: {}", path.path, e);
            } catch (Throwable e) {
                continue;
            }

            try {
                zkClient.addEphemeralNode(path.path);
                return true;
            } catch (KeeperException.NodeExistsException e) {
                log.debug("zk node already exist. path: {}", path.path, e);
            } catch (Throwable e) {
                log.error("registe node is {}, add to fail url to retry", path.path);
            }
        }
        return false;
    }

    private void unregist(final Path path) {
        registedUrls.remove(path);
        Action remove = failedActions.remove(path);
        if (remove != null) remove.cancel();

        if (!doUnRegist(path)) {
            AbstractAction failedAction = new AbstractAction(path, false) {
                @Override
                protected void doExec() {
                    unregist(path);
                }
            };
            Action old = failedActions.putIfAbsent(path, failedAction);
            if (old == null) actions.add(failedAction);
            return;
        }

        log.info("消费者 {} 下线", path.path);
    }

    private boolean doUnRegist(Path path) {
        for (int i = 0; i < RETRY_TIME; i++) {
            try {
                if (zkClient.checkExist(path.path))
                    zkClient.deletePath(path.path);

                return true;
            } catch (KeeperException.NoNodeException e) {
                return true;
            } catch (Exception e) {
                log.error("unregist error", e);
            }
        }
        return false;
    }

    public void setAutoOnline(boolean autoOnline) {
        this.isOnline = autoOnline;
    }

    @Override
    public String registry() {
        return info.getRegistry();
    }

    @PreDestroy
    public void destroy() {
        deleteNodeOnZk();
        zkClient.close();
    }

    private void deleteNodeOnZk() {
        if (registedUrls.size() == 0) return;
        Set<Path> copy = new HashSet<>(registedUrls.keySet());
        for (Path p : copy) {
            try {
                zkClient.deletePath(p.path);
            } catch (KeeperException.NoNodeException e) {
                log.info("no node exit on zk node is {}", p);
            } catch (Exception e) {
                log.warn("shutdown server delete node from zk error", e);
            }
        }
    }

    private ZKClient initZK(String zkAddress) {
        ZKClient zkClient = ZKClientCache.get(zkAddress);
        zkClient.addConnectionChangeListenter(new ConnectionStateListener() {
            @Override
            public void stateChanged(ZKClient client, ConnectionState newState) {
                switch (newState) {
                    case RECONNECTED: {
                        log.info("Reconnected to zookeeper");
                        if (!isOnline) return;

                        try {
                            Set<Path> copy = new HashSet<Path>(registedUrls.keySet());
                            if (copy.size() == 0) return;
                            for (Path p : copy) {
                                regist(p);
                            }
                        } catch (Exception e) {
                            log.debug("register after reconnected failed.", e);
                        }
                    }
                    break;
                    case SUSPENDED: {
                        log.info("Connection to zookeeper lost, attempt connect ");
                    }
                    break;
                    case LOST: {
                        log.info("Lost connection to zookeeper");
                    }
                    break;
                }
            }
        });
        return zkClient;
    }

    private void initRecover() {
        ManagedExecutors.getScheduleExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    retry();
                } catch (Throwable t) {
                    log.error("connect error", t);
                }
            }
        }, RECOVER_TIME, RECOVER_TIME, TimeUnit.SECONDS);
    }

    private void retry() {
        for (Action action : actions) {
            if (action.isOnline() == isOnline) {
                action.exec();
            }
        }
    }

    private static class Path {
        public final String parent;

        public final String path;

        Path(String parent, String path) {
            this.parent = parent;
            this.path = path;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Path path1 = (Path) o;

            if (parent != null ? !parent.equals(path1.parent) : path1.parent != null) return false;
            if (path != null ? !path.equals(path1.path) : path1.path != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = parent != null ? parent.hashCode() : 0;
            result = 31 * result + (path != null ? path.hashCode() : 0);
            return result;
        }
    }

    private interface Action {
        Path path();

        boolean isOnline();

        void exec();

        void cancel();
    }

    private static abstract class AbstractAction implements Action {

        private volatile boolean cancel = false;

        private final Path path;
        private final boolean isOnline;

        AbstractAction(Path path, boolean isOnline) {
            this.path = path;
            this.isOnline = isOnline;
        }

        public void cancel() {
            cancel = true;
        }

        public void exec() {
            if (cancel) return;
            doExec();
        }

        protected abstract void doExec();

        public Path path() {
            return this.path;
        }

        public boolean isOnline() {
            return this.isOnline;
        }
    }
}
