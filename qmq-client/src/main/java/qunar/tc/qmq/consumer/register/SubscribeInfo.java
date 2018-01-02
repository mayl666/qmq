package qunar.tc.qmq.consumer.register;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.google.common.base.Strings;
import qunar.tc.qmq.utils.Constants;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yunfeng.yang
 * @since 2017/1/16
 */
class SubscribeInfo {

    private final String host;

    private final int servicePort;

    private final String appCode;

    private final String registry;

    private String subjectRoot = Constants.CONSUMER_SUBJECT_ROOT;

    private String version;

    private final ConcurrentMap<String, ExecutorConfig> executorConfigMap = new ConcurrentHashMap<>();

    SubscribeInfo(String zkAddress, String host, int servicePort, String subjectRoot, String appCode, String version) {
        this.registry = zkAddress;
        this.host = host;
        this.servicePort = servicePort;
        this.appCode = appCode;
        this.version = version;

        if (!Strings.isNullOrEmpty(subjectRoot)) {
            this.subjectRoot = subjectRoot;
        }
    }

    String buildParentPath(String prefix, String group) {
        return subjectRoot + Constants.ZK_PATH_SEP + prefix + Constants.ZK_PATH_SEP + group;
    }

    String buildFinalPath(String prefix, String parentPath, ExecutorConfig executorConfig) {
        String url = buildConsumerUrl(prefix, executorConfig);
        return parentPath + Constants.ZK_PATH_SEP + URL.encode(url);
    }

    String buildConsumerUrl(String prefix, ExecutorConfig executorConfig) {
        URL url = new URL(Constants.PROTOCOL, host, servicePort);
        Map<String, String> parameters = new HashMap<>();

        parameters.put(Constants.APP, appCode);
        parameters.put(Constants.SELECTOR, prefix);
        parameters.put(Constants.VERSION, version);
        parameters.put(Constants.REVERSION, Version.getVersion(this.getClass(), version));
        parameters.put(Constants.QUEUE_SIZE, executorConfig.queueSize);
        parameters.put(Constants.MIN_THREAD, executorConfig.minThread);
        parameters.put(Constants.MAX_THREAD, executorConfig.maxThread);
        url = url.addParameters(parameters);
        return url.toFullString();
    }

    public String getRegistry() {
        return registry;
    }

    void addExecutorConfig(String key, ExecutorConfig executor) {
        this.executorConfigMap.put(key, executor);
    }

    ExecutorConfig getExecutorConfig(String key) {
        return this.executorConfigMap.get(key);
    }

    public String getAppCode() {
        return appCode;
    }
}
