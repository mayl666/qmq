package qunar.tc.qmq.utils;

import qunar.tc.qconfig.client.Feature;
import qunar.tc.qconfig.client.MapConfig;

import java.util.Map;

/**
 * Created by zhaohui.yu
 * 16/8/17
 */
public class RegistryUtils {

    private static Map<String, String> registries;

    static {
        registries = MapConfig.get("tc_public_config", "registry.properties", Feature.DEFAULT).asMap();
    }

    public static String resolve(String room) {
        return registries.get(room);
    }
}
