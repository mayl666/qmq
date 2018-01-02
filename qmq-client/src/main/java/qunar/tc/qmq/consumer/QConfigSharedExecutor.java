package qunar.tc.qmq.consumer;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Conf;
import qunar.concurrent.PooledExecutor;
import qunar.concurrent.SharedExecutor;
import qunar.tc.qconfig.client.Feature;
import qunar.tc.qconfig.client.TypedConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static qunar.tc.qmq.config.QConfigConstant.QMQ_CLINET_GROUP;
import static qunar.tc.qmq.config.QConfigConstant.SHARD_EXECUTOR_CONFIG;

/**
 * User: zhaohuiyu
 * Date: 10/31/14
 * Time: 3:14 PM
 */
class QConfigSharedExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QConfigSharedExecutor.class);
    private static final Splitter SPLITTER = Splitter.on('|').trimResults().omitEmptyStrings();

    //自己应用里的配置，优先级高
    private static final ConcurrentMap<String, Object> localConfig = new ConcurrentHashMap<>();

    //所有配置文件里有，但是实际没有使用的配置
    private static final ConcurrentMap<String, Object> executorPreConfig = new ConcurrentHashMap<>();

    static final String GLOBAL_CONFIG = "default";
    static final String CORE_CONFIG = "pool.core.size";
    static final String MAX_CONFIG = "pool.max.size";

    private static final ImmutableSet<String> WHITESETS = ImmutableSet.of(GLOBAL_CONFIG, CORE_CONFIG, MAX_CONFIG);

    private SharedExecutor executors;
    private final ThreadPoolExecutorManager executorManager;

    QConfigSharedExecutor() {
//        executors = new SharedExecutor("quota", 10000, 12, 100);
//        executors.setDefaultBlockWait(-1);
        executorManager = new ThreadPoolExecutorManager("qmq-msg-handler-");
    }

    public void init() {
        sanityCheck();
        initThreadPoolExecutorConfig();
    }

    private void sanityCheck() {
        try {
            SharedExecutor.class.getDeclaredField("defaultRejectedExecutionHandler");
        } catch (NoSuchFieldException e) {
            String message = "你使用的common包版本过低，不支持当前qmq-client版本，请更新至8.1.13及以上版本: http://wiki.corp.qunar.com/pages/viewpage.action?pageId=63243158";
            LOGGER.error(message);
            throw new IllegalStateException(message);
        }
    }

    private void initSharedExecutorConfig() {
        SharedExecutorConfigParser globalConfigParser = new SharedExecutorConfigParser(executors, false);//qconfig-qmq-client使用
        SharedExecutorConfigParser localConfigParser = new SharedExecutorConfigParser(executors, true);//本地使用
        try {
            TypedConfig.get(SHARD_EXECUTOR_CONFIG, Feature.DEFAULT, localConfigParser).current();
        } catch (Throwable e) {
            LOGGER.debug("没有找到自定义线程池配置文件quota.properties,将使用默认配置: http://wiki.corp.qunar.com/pages/viewpage.action?pageId=64114065");
        }
        TypedConfig.get(QMQ_CLINET_GROUP, SHARD_EXECUTOR_CONFIG, Feature.DEFAULT, globalConfigParser).current();
    }

    private void initThreadPoolExecutorConfig() {
        ThreadPoolExecutorConfigParser parser = new ThreadPoolExecutorConfigParser(executorManager);
        TypedConfig.get(QMQ_CLINET_GROUP, SHARD_EXECUTOR_CONFIG, Feature.DEFAULT, parser).current();
    }

    Executor getExecutor(String key) {
        return getThreadPoolExecutor(key);
    }

    private Executor getSharedExecutor(String key) {
        PooledExecutor executor = executors.getExecutor(key);
        int[] config = (int[]) executorPreConfig.remove(key);
        if (config == null) return executor;

        executor.setMinThreads(config[0]);
        executor.setMaxThreads(config[1]);
        executor.resizeQueue(config[2]);
        return executor;
    }

    private Executor getThreadPoolExecutor(String key) {
        return executorManager.getExecutor(key);
    }

    private static class SharedExecutorConfigParser extends TypedConfig.MapParser<Object> {
        private SharedExecutor executors;
        private boolean isHighPriority;

        SharedExecutorConfigParser(SharedExecutor executors, boolean isHighPriority) {
            this.executors = executors;
            this.isHighPriority = isHighPriority;
        }

        @Override
        protected Object parse(Map<String, String> map) {
            if (map == null || map.size() == 0) {
                return null;
            }

            Conf conf = Conf.fromMap(map);

            int coreSize = conf.getInt(CORE_CONFIG, executors.getCorePoolSize());
            if (handlePriority(map, CORE_CONFIG, coreSize) && coreSize != executors.getCorePoolSize()) {
                executors.setCorePoolSize(coreSize);
            }

            int maxSize = conf.getInt(MAX_CONFIG, executors.getMaximumPoolSize());
            if (handlePriority(map, MAX_CONFIG, maxSize) && maxSize != executors.getMaximumPoolSize()) {
                executors.setMaximumPoolSize(maxSize);
            }

            int[] result = parseLine(map.get(GLOBAL_CONFIG), new int[]{executors.getDefaultMinThreads(),
                    executors.getDefaultMaxThreads(), executors.getDefaultQueueSize()});

            if (handlePriority(map, GLOBAL_CONFIG, result)) {
                executors.setDefaultMinThreads(result[0]);
                executors.setDefaultMaxThreads(result[1]);
                executors.setDefaultQueueSize(result[2]);
                LOGGER.info("默认线程池配置, min: {}, max: {}, queue: {}", result[0], result[1], result[2]);
            }

            for (Map.Entry<String, String> entry : map.entrySet()) {
                String name = entry.getKey();
                if (WHITESETS.contains(name)) continue;

                int[] value = parseLine(entry.getValue(), result);
                if (handlePriority(map, name, value)) {
                    LOGGER.info("给subject prefix: {} 的线程池配置, min: {}, max: {}, queue: {}", name, value[0], value[1], value[2]);

                    //只有那些已经使用的queue才创建，但是对于没有使用的queue会先将其配置缓存起来
                    //如果后面创建的时候可以使用
                    PooledExecutor executor = executors.getIfExists(name);
                    if (executor == null) {
                        executorPreConfig.put(name, value);
                        continue;
                    }
                    executor.setMinThreads(value[0]);
                    executor.setMaxThreads(value[1]);
                    executor.resizeQueue(value[2]);
                }
            }

            return null;
        }

        private boolean handlePriority(Map<String, String> newConf, String key, Object value) {
            if (!newConf.containsKey(key)) {
                return false;
            }
            if (isHighPriority) {
                localConfig.put(key, value);//cache住，使用新的值
                return true;
            } else {
                if (!localConfig.containsKey(key)) {
                    return true;//cache没有，使用新的
                } else {
                    return false;//cache有，不更新
                }
            }
        }
    }

    private static final class ThreadPoolExecutorConfigParser extends TypedConfig.MapParser<Object> {
        private final ThreadPoolExecutorManager executorManager;

        ThreadPoolExecutorConfigParser(ThreadPoolExecutorManager executorManager) {
            this.executorManager = executorManager;
        }

        @Override
        protected Object parse(Map<String, String> map) {
            final int[] newConfig = parseLine(map.get(GLOBAL_CONFIG), configToValue(executorManager.getDefaultConfig()));
            executorManager.setDefaultExecutorConfig(valueToConfig(newConfig));

            for (Map.Entry<String, String> entry : map.entrySet()) {
                String name = entry.getKey();
                if (WHITESETS.contains(name)) continue;

                int[] value = parseLine(entry.getValue(), newConfig);
                if (value != newConfig) {
                    executorManager.setAndUpdateExecutorConfig(entry.getKey(), valueToConfig(value));
                }
            }
            return null;
        }
    }

    private static int[] parseLine(String line, int[] defaultValue) {
        if (Strings.isNullOrEmpty(line)) {
            return defaultValue;
        }

        List<String> list = SPLITTER.splitToList(line);
        if (list.size() != 3) {
            LOGGER.warn("配置格式不正确: {}", line);
            return defaultValue;
        }
        int[] result = new int[3];
        try {
            for (int i = 0; i < list.size(); i++) {
                result[i] = Integer.parseInt(list.get(i));
            }
            return result;
        } catch (Exception e) {
            LOGGER.warn("配置格式不正确: {}", line);
            return defaultValue;
        }
    }

    private static int[] configToValue(ThreadPoolExecutorManager.ExecutorConfig config) {
        return new int[]{config.coreSize, config.maxSize, config.queueSize};
    }

    private static ThreadPoolExecutorManager.ExecutorConfig valueToConfig(int[] value) {
        return new ThreadPoolExecutorManager.ExecutorConfig(value[0], value[1], value[2]);
    }
}
