package qunar.tc.qmq.config;

import com.google.common.base.Strings;
import qunar.tc.qconfig.client.Configuration;
import qunar.tc.qconfig.client.Feature;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.common.AtomicConfig;
import qunar.tc.qmq.common.AtomicIntegerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yiqun.fan create on 17-8-17.
 */
public class PullSubjectsConfig {
    private static final PullSubjectsConfig config = new PullSubjectsConfig();

    public static PullSubjectsConfig get() {
        return config;
    }

    private final Map<ConfigType, AtomicConfig> configMap;
    private final AtomicIntegerConfig pullBatchSizeConfig;
    private final AtomicIntegerConfig pullTimeoutConfig;
    private final AtomicIntegerConfig ackNosendLimit;
    private final AtomicIntegerConfig maxRetryNum;
    private final AtomicIntegerConfig pullState;

    private PullSubjectsConfig() {
        pullBatchSizeConfig = new AtomicIntegerConfig(20, Integer.MIN_VALUE, Integer.MAX_VALUE);
        pullTimeoutConfig = new AtomicIntegerConfig(1000, 1000, Integer.MAX_VALUE);
        ackNosendLimit = new AtomicIntegerConfig(50, Integer.MIN_VALUE, Integer.MAX_VALUE);
        maxRetryNum = new AtomicIntegerConfig(BaseMessage.DEFAULT_MAX_RETRY_NUM, 0, Integer.MAX_VALUE);
        pullState = new AtomicIntegerConfig(PullState.PULLING.ordinal(), 0, Integer.MAX_VALUE);
        configMap = new HashMap<>();
        configMap.put(ConfigType.PULL_BATCHSIZE, pullBatchSizeConfig);
        configMap.put(ConfigType.PULL_TIMEOUT, pullTimeoutConfig);
        configMap.put(ConfigType.ACK_NOSEND_LIMIT, ackNosendLimit);
        configMap.put(ConfigType.MAX_RETRY_NUM, maxRetryNum);
        configMap.put(ConfigType.PULL_STATE, pullState);
        loadConfig();
    }

    private void loadConfig() {
        MapConfig config = MapConfig.get(QConfigConstant.QMQ_CLINET_GROUP, QConfigConstant.PULL_SUBJECTS_CONFIG, Feature.DEFAULT);
        config.addListener(new Configuration.ConfigListener<Map<String, String>>() {
            @Override
            public void onLoad(Map<String, String> conf) {
                for (Map.Entry<String, String> entry : conf.entrySet()) {
                    if (Strings.isNullOrEmpty(entry.getKey()) || Strings.isNullOrEmpty(entry.getValue())) {
                        continue;
                    }
                    for (ConfigType configType : ConfigType.values()) {
                        String key = entry.getKey();
                        if (key.endsWith(configType.suffix)) {
                            AtomicConfig atomicConfig = configMap.get(configType);
                            String subject = key.substring(0, key.length() - configType.suffix.length());
                            atomicConfig.setString(subject, entry.getValue());
                            break;
                        }
                    }
                }
            }
        });
        config.asMap();
    }

    public AtomicReference<Integer> getPullBatchSize(String subject) {
        return pullBatchSizeConfig.get(subject);
    }

    public AtomicReference<Integer> getPullTimeout(String subject) {
        return pullTimeoutConfig.get(subject);
    }

    public AtomicReference<Integer> getAckNosendLimit(String subject) {
        return ackNosendLimit.get(subject);
    }

    public AtomicReference<Integer> getMaxRetryNum(String subject) {
        return maxRetryNum.get(subject);
    }

    public AtomicReference<Integer> getPullState(String subject) {
        return pullState.get(subject);
    }

    private enum ConfigType {
        PULL_BATCHSIZE("_pullBatchSize"),
        PULL_TIMEOUT("_pullTimeout"),
        ACK_TIMEOUT("_ackTimeout"),
        ACK_NOSEND_LIMIT("_ackNosendLimit"),
        MAX_RETRY_NUM("_maxRetryNum"),
        PULL_STATE("_pullState");

        private final String suffix;

        ConfigType(String suffix) {
            this.suffix = suffix;
        }
    }
}
