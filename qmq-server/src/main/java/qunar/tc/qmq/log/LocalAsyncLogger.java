package qunar.tc.qmq.log;

import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Closer;
import qunar.tc.qconfig.client.Configuration;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.batch.ExecutorUtil;
import qunar.tc.qmq.batch.Flusher;
import qunar.tc.qmq.batch.Processor;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaohui.yu
 * 16/4/27
 */
public class LocalAsyncLogger implements Processor<LocalAsyncLogger.Record> {
    private static final Logger LOG = LoggerFactory.getLogger(LocalAsyncLogger.class);

    private static final int batchSize = 1000;

    private static final int QUEUE_SIZE = 10000;

    private static final int THREADS = 1;

    private static final int FLUSH_INTERVAL = 2000;

    //在schedule里单线程使用
    private final SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd-HH");

    private volatile PrintWriter writer;

    private final Flusher<Record> flusher;

    private final String directory;

    private final String suffix;

    private volatile String fileName = "";

    private volatile boolean logSwitch = true;

    public LocalAsyncLogger(String prefix, String suffix) {
        this.directory = prefix;
        this.suffix = suffix;

        MapConfig.get("logswitch.properties").addListener(new Configuration.ConfigListener<Map<String, String>>() {
            @Override
            public void onLoad(Map<String, String> conf) {
                logSwitch = Boolean.valueOf(conf.get("switch"));
            }
        });

        rollOnDemand();
        ExecutorUtil.SHARED_SCHEDULED.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                rollOnDemand();
            }
        }, 0, 1, TimeUnit.MINUTES);

        this.flusher = new Flusher<>("qmq-log", batchSize, FLUSH_INTERVAL, QUEUE_SIZE, THREADS, this);
    }

    @Override
    public void process(List<Record> items) {
        try {
            for (Record record : items) {
                writer.println(record.time + ":" + record.message);
            }
            writer.flush();
        } catch (Throwable e) {
            LOG.debug("write log record failed.", e);
        }
    }

    public void log(String message) {
        if (!logSwitch) return;
        flusher.add(new Record(System.currentTimeMillis(), message));
    }

    public void log(BaseMessage message, String content) {
        log("[" + message.getMessageId() + ":" + message.getSubject() + "]\t" + content);
    }

    public void log(String messageId, String subject, String content) {
        log("[" + messageId + ":" + subject + "]\t" + content);
    }

    private void rollOnDemand() {
        String fileName = formater.format(new Date());
        if (fileName.equals(this.fileName)) return;

        PrintWriter origin = this.writer;
        //只有打开失败才算切换成功，关闭旧的文件
        if (open(fileName)) {
            this.fileName = fileName;
            close(origin);
        }

    }

    private boolean open(String fileName) {
        try {
            File path = new File(directory + fileName + suffix);
            path.getParentFile().mkdirs();
            writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path, true), Charsets.UTF_8), 128000), false);
            return true;
        } catch (Throwable t) {
            LOG.debug("open file error", t);
            return false;
        }
    }

    private synchronized void close(PrintWriter writer) {
        Closer.close(writer);
    }

    public static final class Record {
        public final long time;
        public final String message;

        public Record(long time, String message) {
            this.time = time;
            this.message = message;
        }
    }
}
