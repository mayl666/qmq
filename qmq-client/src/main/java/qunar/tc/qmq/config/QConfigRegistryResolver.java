package qunar.tc.qmq.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.AppServer;
import qunar.agile.Strings;
import qunar.management.ServerManager;
import qunar.tc.qconfig.client.Feature;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.producer.RegistryResolver;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static qunar.tc.qmq.config.QConfigConstant.PUBLIC_CONFIG;
import static qunar.tc.qmq.config.QConfigConstant.REGISTRY_CONFIG_FILE;

/**
 * User: zhaohuiyu
 * Date: 10/28/14
 * Time: 2:31 PM
 */
public class QConfigRegistryResolver implements RegistryResolver {
    private static final Logger logger = LoggerFactory.getLogger(QConfigRegistryResolver.class);

    private static final Pattern pattern = Pattern.compile("\\.(cn(\\d))\\.");

    public static final RegistryResolver INSTANCE = new QConfigRegistryResolver();

    private final Map<String, String> registries;

    private String dataCenter;

    private QConfigRegistryResolver() {
        registries = MapConfig.get(PUBLIC_CONFIG, REGISTRY_CONFIG_FILE, Feature.create().autoReload(false).build()).asMap();
        resolveDataCenter();
    }

    @Override
    public String resolve() {
        return registries.get(dataCenter);
    }

    @Override
    public String resolve(String dataCenter) {
        return registries.get(dataCenter);
    }

    @Override
    public String dataCenter() {
        return dataCenter;
    }

    @Override
    public Set<String> list() {
        return new HashSet<String>(registries.values());
    }


    //判断哪个机房
    //dev和beta都是cn6
    //对于线上，则根据hostname判断出所属机房
    private void resolveDataCenter() {
        if (dataCenter != null) return;

        AppServer.Type type = env();

        if (type != AppServer.Type.prod) {
            dataCenter = "cn6";
            return;
        }

        try {
            String room = ServerManager.getInstance().getAppConfig().getServer().getRoom();
            if (room == null) {
                dataCenter = cn(hostname());
            } else {
                dataCenter = room;
            }
        } catch (Exception e) {
            dataCenter = cn(hostname());
        }
    }

    private String hostname() {
        String hostName = null;
        try {
            hostName = ServerManager.getInstance().getAppConfig().getServer().getHostname();
        } catch (Exception e) {
            logger.error("无法识别当前应用环境，请检查应用中心相关配置");
            throw new RuntimeException(e);

        }
        if (hostName == null) {
            logger.error("无法识别当前应用环境，请检查应用中心相关配置");
            throw new RuntimeException("无法识别当前应用环境");
        }
        return hostName;
    }

    private AppServer.Type env() {
        AppServer.Type type = null;
        try {
            type = ServerManager.getInstance().getAppConfig().getServer().getType();
        } catch (Exception e) {
            logger.error("无法识别当前应用环境，请检查应用中心相关配置");
            throw new RuntimeException(e);

        }
        if (type == null) {
            logger.error("无法识别当前应用环境，请检查应用中心相关配置");
            throw new RuntimeException("无法识别当前应用环境");
        }
        return type;
    }

    private String cn(String host) {

        if (Strings.isEmpty(host)) {
            return null;
        }

        Matcher matcher = pattern.matcher(host);
        if (matcher.find()) {
            return matcher.group(1);
        }

        return null;
    }
}
