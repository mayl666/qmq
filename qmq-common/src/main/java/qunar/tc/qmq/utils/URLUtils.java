package qunar.tc.qmq.utils;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by IntelliJ IDEA.
 * User: liuzz
 * Date: 12-12-18
 * Time: 上午10:31
 */
public final class URLUtils {

    private static final String JDBC_PREFIX = "jdbc:";

    private static final AtomicInteger index = new AtomicInteger(0);

    private static final String REGISTRY_FILE_CACHE_PATH = System.getProperty("catalina.base") + "/logs/dubbo-registry-qmq-%s.cache";

    public static URI parseUrl(String url) {
        Preconditions.checkNotNull(url);
        if (url.startsWith(JDBC_PREFIX)) {
            url = url.substring(JDBC_PREFIX.length());
        }
        return URI.create(url);
    }

    public static String filterFile(String urlKey) {
        URL url = URL.valueOf(urlKey);
        url = url.removeParameter("file")
                .addParameter("file", String.format(REGISTRY_FILE_CACHE_PATH, index.incrementAndGet()));
        return url.toFullString();
    }

    public static String buildZKUrl(String address, String group) {
        return buildZKUrl(address, ImmutableMap.of(Constants.GROUP_KEY, group));
    }

    public static String buildZKUrl(String address, Map<String, String> parameters) {
        Map<String, String> copy = new HashMap<String, String>(parameters);
        copy.put(com.alibaba.dubbo.common.Constants.PROTOCOL_KEY, "zookeeper");
        return com.alibaba.dubbo.common.utils.UrlUtils.parseURL(address, copy).toString();
    }
}
