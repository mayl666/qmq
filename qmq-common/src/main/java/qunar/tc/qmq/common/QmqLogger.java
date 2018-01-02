/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.common;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.AsyncLogger;
import qunar.Corporation;
import qunar.management.ServerManager;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.utils.PropertiesLoader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-6
 */
public class QmqLogger {
    private static final Logger LOG = LoggerFactory.getLogger(QmqLogger.class);

    private static final String FALLBACK_LOG_DIR = "target";

    private static final AsyncLogger LOGGER;

    static {
        final String home = defaultLogDirectory();
        LOGGER = Factory.create(home);

        LOG.info("dubbo access logging in : " + home);
    }

    private static String defaultLogDirectory() {
        try {
            return getLogDirectory().getCanonicalPath();
        } catch (IOException e) {
            LOG.debug("get logger home path failed: ", e);
        }

        return FALLBACK_LOG_DIR;
    }

    private static File getLogDirectory() {
        final Corporation corp = ServerManager.getInstance().getCorporation();
        switch (corp) {
            case CTRIP:
                return getCtripLogDirectory();
            default:
                return getCatalinaDefaultLogDirectory();
        }
    }

    private static File getCtripLogDirectory() {
        String home = System.getProperty("APPLOGDIR");
        if (!Strings.isNullOrEmpty(home)) {
            return new File(home);
        }

        home = System.getProperty("log.server");
        if (!Strings.isNullOrEmpty(home)) {
            return new File(home);
        }

        return getCatalinaDefaultLogDirectory();
    }

    private static File getCatalinaDefaultLogDirectory() {
        final String home = System.getProperty("catalina.base");
        if (!Strings.isNullOrEmpty(home)) {
            return new File(home, "logs");
        }

        return new File(FALLBACK_LOG_DIR);
    }

    public static void log(Message message, String content) {
        LOGGER.log("[" + message.getMessageId() + ":" + message.getSubject() + "]\t" + content);
    }

    public static void log(BaseMessage message, String content) {
        LOGGER.log("[" + message.getMessageId() + ":" + message.getSubject() + "]\t" + content);
    }

    public static void log(String messageId, String subject, String content) {
        LOGGER.log("[" + messageId + ":" + subject + "]\t" + content);
    }

    public static void log(String info) {
        LOGGER.log(info);
    }

    private static class Factory {
        private static AsyncLogger create(String home) {
            Properties parent = System.getProperties();
            Properties bundle = PropertiesLoader.load("qmq-logger.properties", parent);
            if (bundle == null) {
                LOG.info("没有找到qmq-logger.properties, 采用默认设置");
                return new AsyncLogger(home + "/qmq.", ".log");

            } else {
                String pattern = bundle.getProperty("roll-pattern");
                if (Strings.isNullOrEmpty(pattern)) {
                    return new AsyncLogger(home + "/qmq.", ".log");
                } else {
                    try {
                        return createInternal(home, pattern);
                    } catch (Throwable e) {
                        LOG.error("如果要自定义qmq日志的滚动方式，common-core的版本的最低要求为: 8.1.12, wiki: http://wiki.corp.qunar.com/pages/viewpage.action?pageId=63243158", e);
                        return new AsyncLogger(home + "/qmq.", ".log");
                    }
                }
            }
        }

        private static AsyncLogger createInternal(String home, String pattern) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
            Class<?> clazz = Class.forName("qunar.AsyncLogger$RollingPattern");
            Constructor<AsyncLogger> constructor = AsyncLogger.class.getConstructor(new Class[]{String.class, String.class, clazz});
            try {
                Class<?> rollingPatternClazz = Class.forName("qunar.DateRollingPattern");
                Constructor<?> ctor = rollingPatternClazz.getConstructor(String.class);
                return constructor.newInstance(home + "/qmq.", ".log", ctor.newInstance(pattern));
            } catch (ClassNotFoundException e) {
                Class<?> rollingPatternClazz = Class.forName("qunar.AsyncLogger$TimeBasedRollingPattern");
                Constructor<?> ctor = rollingPatternClazz.getConstructor(String.class);
                return constructor.newInstance(home + "/qmq.", ".log", ctor.newInstance(pattern));
            }
        }
    }


}
