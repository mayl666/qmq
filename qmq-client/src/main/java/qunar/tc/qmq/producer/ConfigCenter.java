package qunar.tc.qmq.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Numbers;
import qunar.tc.qmq.utils.PropertiesLoader;

import java.util.Properties;

/**
 * @author zhenyu.nie created on 2017 2017/7/5 11:57
 */
public class ConfigCenter {

    private static final Logger logger = LoggerFactory.getLogger(ConfigCenter.class);

    private static final ConfigCenter INSTANCE = new ConfigCenter();

    public static ConfigCenter getInstance() {
        return INSTANCE;
    }

    private static final int _1K = 1024;

    private static final int MAX_MESSAGE_SIZE = 60 * _1K;

    private static final int MIN_EXPIRED_TIME = 15;

    private static final int QUEUE_MEM_SIZE = 10000;

    private static final int SEND_THREADS = 2;
    private static final int SEND_BATCH = 20;
    private static final int SEND_TRY_COUNT = 2;

    private static final String DEFAULT_ASYNC_SEND = "false";

    private int maxQueueSize;
    private int sendThreads;
    private int sendBatch;
    private int sendTryCount;

    private boolean syncSend = false;

    private ConfigCenter() {
        Properties parent = System.getProperties();

        Properties bundle = PropertiesLoader.load("qmq-producer.properties", parent);

        if (bundle == null) {
            bundle = new Properties(parent); // 不直接使用parent, 是为了避免后期改动是无意中修改了全局属性.
            logger.debug("没有找到qmq-producer.properties, 采用默认设置");
        }

        maxQueueSize = Numbers.toInt(bundle.getProperty("queue.mem.size"), QUEUE_MEM_SIZE);
        sendThreads = Numbers.toInt(bundle.getProperty("queue.send.threads"), SEND_THREADS);
        sendBatch = Numbers.toInt(bundle.getProperty("queue.send.batch"), SEND_BATCH);
        sendTryCount = Numbers.toInt(bundle.getProperty("queue.send.try"), SEND_TRY_COUNT);

        // 默认使用异步发送，业务可以通过开关打开同步发送
        syncSend = Boolean.valueOf(bundle.getProperty("queue.send.sync", DEFAULT_ASYNC_SEND));
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public int getSendThreads() {
        return sendThreads;
    }

    public void setSendThreads(int sendThreads) {
        this.sendThreads = sendThreads;
    }

    public int getSendBatch() {
        return sendBatch;
    }

    public void setSendBatch(int sendBatch) {
        this.sendBatch = sendBatch;
    }

    public int getSendTryCount() {
        return sendTryCount;
    }

    public void setSendTryCount(int sendTryCount) {
        this.sendTryCount = sendTryCount;
    }

    public boolean isSyncSend() {
        return syncSend;
    }

    public int getMaxMessageSize() {
        return MAX_MESSAGE_SIZE;
    }

    public int getMinExpiredTime() {
        return MIN_EXPIRED_TIME;
    }
}
