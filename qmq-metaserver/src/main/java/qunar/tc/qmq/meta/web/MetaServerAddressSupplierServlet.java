package qunar.tc.qmq.meta.web;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import qunar.agile.Conf;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.utils.IPUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

import static qunar.tc.qmq.meta.startup.ServerWrapper.DEFAULT_META_SERVER_PORT;

/**
 * @author yunfeng.yang
 * @since 2017/9/1
 */
public class MetaServerAddressSupplierServlet extends HttpServlet {
    private final String localHost;
    private final Conf conf;

    public MetaServerAddressSupplierServlet() {
        final Map<String, String> configMap = MapConfig.get("config.properties").asMap();
        this.conf = Conf.fromMap(configMap);
        this.localHost = IPUtil.getLocalHost();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(localHost), "get localhost error");
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.getWriter().write(buildLocalAddress());
    }

    private String buildLocalAddress() {
        return localHost + ":" + conf.getInt("meta.server.port", DEFAULT_META_SERVER_PORT);
    }
}
