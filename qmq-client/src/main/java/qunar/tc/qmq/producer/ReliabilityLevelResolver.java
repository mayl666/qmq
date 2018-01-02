package qunar.tc.qmq.producer;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qconfig.client.Configuration;
import qunar.tc.qconfig.client.Feature;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.ReliabilityLevel;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhaohui.yu
 * 15/11/3
 */
class ReliabilityLevelResolver {
    private static final Logger logger = LoggerFactory.getLogger(ReliabilityLevel.class);

    private volatile Map<String, ReliabilityLevel> reliabilityLevelConfig;

    private static final Map<String, ReliabilityLevel> map = new HashMap<String, ReliabilityLevel>();

    static {
        map.put("l", ReliabilityLevel.Low);
        map.put("m", ReliabilityLevel.Middle);
        map.put("h", ReliabilityLevel.High);
    }

    public ReliabilityLevelResolver() {
        reliabilityLevelConfig = Maps.newHashMap();
        MapConfig config = MapConfig.get("reliabilitylevel.properties", Feature.DEFAULT);
        config.addListener(new Configuration.ConfigListener<Map<String, String>>() {
            @Override
            public void onLoad(Map<String, String> conf) {
                Map<String, ReliabilityLevel> result = new HashMap<String, ReliabilityLevel>();
                for (Map.Entry<String, String> entry : conf.entrySet()) {
                    ReliabilityLevel level = map.get(entry.getValue().toLowerCase());
                    if (level == null) {
                        logger.warn("config error: {} - {}, allowed value: l, m, h", entry.getKey(), entry.getValue());
                        continue;
                    }
                    result.put(entry.getKey(), level);
                }

                reliabilityLevelConfig = result;
            }
        });
    }

    public ReliabilityLevel resolve(Message message) {
        String subject = message.getSubject();
        ReliabilityLevel configred = reliabilityLevelConfig.get(subject);
        return configred == null ? message.getReliabilityLevel() : configred;
    }
}
