package qunar.tc.qmq.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.AppConfig;
import qunar.management.ServerManager;

public class Applications {
    private static final Logger log = LoggerFactory.getLogger(Applications.class);

    private String appCode;
    private String room;

    private static final Applications APPLICATIONS = new Applications();

    public static Applications getInstance() {
        return APPLICATIONS;
    }

    private Applications() {
        ServerManager serverManager = null;
        try {
            serverManager = ServerManager.getInstance();
        } catch (Throwable e) {
            String message = "必须使用common 8.1.4及以上版本，并且在classpath下放置在应用中心申请的qunar-app.properties文件，参考: http://wiki.corp.qunar.com/pages/viewpage.action?pageId=63243146";
            log.error(message, e);
            throw new RuntimeException(message);
        }
        AppConfig appConfig = serverManager.getAppConfig();
        appCode = appConfig.getName();
        room = appConfig.getServer().getRoom();
    }

    public String getAppCode() {
        return appCode;
    }

    public String getRoom() {
        return room;
    }
}
