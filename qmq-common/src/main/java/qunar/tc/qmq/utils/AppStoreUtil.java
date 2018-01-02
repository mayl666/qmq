package qunar.tc.qmq.utils;

import qunar.Corporation;
import qunar.ServiceFinder;
import qunar.management.ServerManagement;
import qunar.util.QunarFileUtil;

import java.io.File;

/**
 * @author keli.wang
 * @since 2017/4/18
 */
public class AppStoreUtil {
    private static final ServerManagement SERVER_MANAGER = ServiceFinder.getService(ServerManagement.class);

    public static File getAppStore() {
        final Corporation corp = SERVER_MANAGER.getCorporation();
        final String app = SERVER_MANAGER.getAppConfig().getName();
        switch (corp) {
            case CTRIP:
                return ctripAppStore(app);
            default:
                return defaultAppStore(app);
        }
    }

    private static File ctripAppStore(final String app) {
        final File path = new File(String.format("/opt/data/%s/config", app));
        if (createCtripAppStore(path)) {
            return path;
        } else {
            return defaultAppStore(app);
        }
    }

    private static boolean createCtripAppStore(File path) {
        if (path.exists()) return true;
        try {
            return path.mkdirs();
        } catch (Throwable e) {
            return false;
        }
    }

    private static File defaultAppStore(final String app) {
        return new File(QunarFileUtil.getQunarStore(), app);
    }
}
