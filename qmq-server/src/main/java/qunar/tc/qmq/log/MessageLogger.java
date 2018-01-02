package qunar.tc.qmq.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Conf;
import qunar.tc.qconfig.client.Configuration.ConfigListener;
import qunar.tc.qconfig.client.Feature;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.protocol.consumer.PullRequest;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * User: zhaohuiyu Date: 1/8/13 Time: 10:47 AM
 */
public class MessageLogger {
    private static final Logger log = LoggerFactory.getLogger(MessageLogger.class);

    private static final LocalAsyncLogger logger;

    private static volatile boolean logSwitch = false;

    static {
        String home = System.getProperty("catalina.base");
        if (home == null)
            home = "target";
        else
            home = home + "/logs";

        try {
            home = new File(home).getCanonicalPath();
        } catch (IOException e) {
            log.debug("logger homepath failed: ", e);
        }

        logger = new LocalAsyncLogger(home + "/qmq.", ".log");

        log.info("logging in : " + home);
        MapConfig mapConfig = MapConfig.get("log_switch.properties", new Feature.Builder().setFailOnNotExists(false).build());
        mapConfig.addListener(new ConfigListener<Map<String, String>>() {

            @Override
            public void onLoad(Map<String, String> map) {
                Conf conf = Conf.fromMap(map);
                logSwitch = conf.getBoolean("switch", false);
            }
        });
    }

    public static String from(String sender) {
        return String.format("from:%s", sender);
    }

    public static String to(String target) {
        return String.format("to:%s", target);
    }

    public static void logAction(String messageId, String subject, Action action) {
        if (logSwitch) {
            logger.log(messageId, subject, "action:" + action);
        }
    }

    public static void logAction(PullRequest pullRequest, Action action) {
        if (logSwitch) {
            logger.log(pullRequest.getSubject(), pullRequest.getGroup(), "request num:" + pullRequest.getRequestNum() + "action:" + action);
        }
    }

}
