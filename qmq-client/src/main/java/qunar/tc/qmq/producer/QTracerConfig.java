package qunar.tc.qmq.producer;

import qunar.tc.qconfig.client.Configuration;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.Message;
import qunar.tc.qtracer.QTracer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhaohui.yu
 * 16/1/22
 */
public class QTracerConfig {

    private volatile Map<String, String[]> keys;

    QTracerConfig() {
        this.keys = Collections.emptyMap();
        MapConfig config = MapConfig.get("qmq-qtracer.properties");
        config.addListener(new Configuration.ConfigListener<Map<String, String>>() {
            @Override
            public void onLoad(Map<String, String> conf) {
                Map<String, String[]> temp = new HashMap<>();
                for (Map.Entry<String, String> entry : conf.entrySet()) {
                    String str = entry.getValue();
                    if (str == null || str.length() == 0) continue;

                    String[] arr = str.trim().split(",");
                    keys.put(entry.getKey(), arr);
                }

                keys = temp;
            }
        });
    }

    public void configure(Message message) {
        String[] properties = this.keys.get(message.getSubject());
        if (properties == null || properties.length == 0) return;

        for (String property : properties) {
            QTracer.addKVAnnotation(property, message.getStringProperty(property));
        }
    }
}
