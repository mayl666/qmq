package qunar.tc.qmq.utils;

import java.nio.charset.Charset;

/**
 * Created by IntelliJ IDEA.
 * User: liuzz
 * Date: 12-12-18
 * Time: 下午12:11
 */
public class Constants {
    public static final String QMQ_ZK_ROOT = "/qmq";

    public static final String QUEUE_SIZE = "queueSize";
    public static final String MIN_THREAD = "minThread";
    public static final String MAX_THREAD = "maxThread";

    public static final String ZK_PATH_SEP = "/";

    /**
     * consumer订阅消息subject时在zookeeper上的路径
     */
    public static final String CONSUMER_SUBJECT_ROOT = QMQ_ZK_ROOT + "/consumer/subject";

    /**
     * broker相关信息根路径
     */
    public static final String BROKER_ROOT = QMQ_ZK_ROOT + "/broker";

    public static final String BACKUP_GROUP_ROOT = QMQ_ZK_ROOT + "/backup/group";

    /**
     * broker配置信息路径
     */
    public static final String BROKER_CONFIG_ROOT = BROKER_ROOT + "/config";

    /**
     * SubscribeService服务所属的分组
     */
    public static final String SUBSCRIBE_GROUP = "default";

    /**
     * broker分组的根路径
     */
    public static final String BROKER_GROUP_ROOT = BROKER_ROOT + "/group";

    public static final String DEFAULT_GROUP = "default";

    public static final String PROTOCOL = "qmq";

    public static final String SELECTOR = "selector";

    public static final String APP = "app";

    public static final String VERSION = "version";

    public static final String VERSION_STORE = "v";

    public static final String REVERSION = "reversion";

    public static final String DEFAULT_VERSION = "1.0.0";

    public static final int MAX_RETRIES = 6;

    public static final String TASK_GROUP_PATH = "/qmq/task/group";

    public static final String DEFAULT_TASK_GROUP = "default";

    public static final String ITERABLE_FILTER_KEY = "iterable";

    public static final Charset UTF8 = Charset.forName("utf-8");
}
