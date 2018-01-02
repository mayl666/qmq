/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.producer;

import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Numbers;
import qunar.concurrent.ManagedThreadPool;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.MessageStore;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.ReliabilityLevel;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.common.QmqLogger;
import qunar.tc.qmq.common.TracedMessage;
import qunar.tc.qmq.utils.PropertiesLoader;
import qunar.tc.qtracer.Constants;

import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-5
 */
class ProduceMessageImpl implements ProduceMessage {
    private static final Logger logger = LoggerFactory.getLogger(ProduceMessageImpl.class);

    private static final int INIT = 0;
    private static final int QUEUED = 1;

    private static final int FINISH = 100;
    private static final int ERROR = -1;
    private static final int BLOCK = -2;
    private static final int WAIT_ENQUEUE_TIMEOUT = 1000 * 10;

    private static final int DEFAULT_THREADS = 10;

    private static final int DEFAULT_QUEUE_SIZE = 1000;

    private static final int threads;
    private static final int queueSize;

    private static final Executor EXECUTOR;

    static {
        Properties parent = System.getProperties();

        Properties bundle = PropertiesLoader.load("qmq-producer.properties", parent);

        if (bundle == null) {
            bundle = new Properties(parent); // 不直接使用parent, 是为了避免后期改动是无意中修改了全局属性.
            logger.info("没有找到qmq-producer.properties, 采用默认设置");
        }

        threads = Numbers.toInt(bundle.getProperty("listener.threas"), DEFAULT_THREADS);
        queueSize = Numbers.toInt(bundle.getProperty("listener.queueSize"), DEFAULT_QUEUE_SIZE);
        EXECUTOR = new ManagedThreadPool(1, threads, 1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(queueSize),
                new NamedThreadFactory("default-send-listener", true),
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        logger.error("MessageSendStateListener任务被拒绝,现在的大小为:threads-{}, queue size-{}.如果在该listener里执行了比较重的操作，请通过qmq-producer.properties调整", threads, queueSize);
                    }
                });

    }

    /**
     * 最多尝试次数
     */
    private int sendTryCount;
    private final TracedMessage base;

    private final QueueSender sender;

    private volatile MessageStore store;
    private volatile String dsIndex;
    private volatile Object storeKey;

    private MessageSendStateListener sendStateListener;

    private final AtomicInteger state = new AtomicInteger(INIT);
    private final AtomicInteger tries = new AtomicInteger(0);
    private boolean syncSend;

    public ProduceMessageImpl(TracedMessage base, QueueSender sender) {
        this.base = base;
        this.sender = sender;
    }

    @Override
    public void save() {
        addTimeAnnotation("begin insert new message");
        store.insertNew(this);
        addTimeAnnotation("inserted message");
    }

    @Override
    public void send() {
        base.message().removeProperty(BaseMessage.keys.qmq_registry);

        if (state.compareAndSet(INIT, QUEUED)) {
            tries.incrementAndGet();
            //当未指定消息存储时，使用同步发送，发送失败会抛出异常
            if (store == null && syncSend) {
                sender.send(this);
                base.addTimeAnnotation("sync enter queue");
            } else if (sender.offer(this)) {
                base.addTimeAnnotation("async enter queue");
                QmqLogger.log(getBase(), "进入发送队列.");
            } else if (store != null) {
                base.addTimeAnnotation("enter queue failed, wait for task");
                QmqLogger.log(getBase(), "内存发送队列已满! 此消息将暂时丢弃,等待task处理");
            } else {
                if (ReliabilityLevel.isLow(getBase())) {
                    base.addTimeAnnotation("enter queue failed, throw");
                    QmqLogger.log(getBase(), "内存发送队列已满! 非可靠消息，此消息被丢弃.");
                    return;
                }
                QmqLogger.log(getBase(), "内存发送队列已满! 此消息在用户进程阻塞,等待队列激活.");

                if (sender.offer(this, WAIT_ENQUEUE_TIMEOUT)) {
                    base.addTimeAnnotation("enter queue succ");
                    QmqLogger.log(getBase(), "重新入队时成功进入发送队列.");
                } else {
                    base.addTimeAnnotation("enter queue failed");
                    QmqLogger.log(getBase(), "由于无法入队,发送失败！取消发送!");
                }
            }
        } else
            throw new IllegalStateException("同一条消息不能被入队两次.");

    }

    @Override
    public void finish() {
        state.set(FINISH);
        if (store != null) {
            base.addTimeAnnotation("process by server");
            store.finish(this);
            base.addTimeAnnotation("finish message");
        }
        onSuccess();
    }

    private void onSuccess() {
        base.close();
        if (sendStateListener == null) return;
        EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                sendStateListener.onSuccess(base.message());
            }
        });
    }

    @Override
    public void error(Exception e) {
        base.addAnnotation(Constants.EXCEPTION_KEY, e.getMessage());
        base.addTimeAnnotation("process error by server");
        state.set(ERROR);
        if (store != null)
            store.error(this);
        if (tries.get() < sendTryCount) {
            QmqLogger.log(base.message(), "发送失败, 重新发送. tryCount: " + tries.get());
            base.addTimeAnnotation("begin resend " + tries.get());
            resend();
        } else {
            base.addTimeAnnotation("resend failed");
            onFailed();
            String message = "发送失败, 已尝试" + tries.get() + "次不再尝试重新发送.";
            QmqLogger.log(base.message(), message);
            if (store == null && syncSend) {
                throw new RuntimeException(message);
            }
        }
    }

    @Override
    public void block() {
        state.set(BLOCK);
        if (store != null)
            store.block(this);
        onFailed();
        base.addTimeAnnotation("reject by server");
        QmqLogger.log(base.message(), "消息被拒绝");
        if (store == null && syncSend) {
            throw new RuntimeException("消息被拒绝且没有store可恢复,请检查应用授权配置");
        }
    }

    @Override
    public void failed() {
        state.set(ERROR);
        if (store != null)
            store.error(this);
        onFailed();
        String message = "消息发送到broker超时";
        QmqLogger.log(base.message(), message);
        if (store == null && syncSend) {
            throw new RuntimeException(message);
        }
    }

    private void onFailed() {
        base.close();
        if (sendStateListener == null) return;
        EXECUTOR.execute(new Runnable() {
            @Override
            public void run() {
                sendStateListener.onFailed(base.message());
            }
        });
    }

    private void resend() {
        state.set(INIT);
        send();
    }

    @Override
    public String getMessageId() {
        return base.message().getMessageId();
    }

    @Override
    public String getSubject() {
        return base.message().getSubject();
    }

    @Override
    public void setStore(MessageStore store) {
        this.store = store;
    }

    @Override
    public BaseMessage getBase() {
        return base.message();
    }

    public void setSendTryCount(int sendTryCount) {
        this.sendTryCount = sendTryCount;
    }

    public void setSendStateListener(MessageSendStateListener sendStateListener) {
        this.sendStateListener = sendStateListener;
    }

    public void setSyncSend(boolean syncSend) {
        this.syncSend = syncSend;
    }

    public void addAnnotation(String key, String value) {
        base.addAnnotation(key, value);
    }

    public void addTimeAnnotation(String msg) {
        base.addTimeAnnotation(msg);
    }

    public void closeTrace() {
        base.close();
    }

    @Override
    public void setDsIndex(String dsIndex) {
        this.dsIndex = dsIndex;
    }

    @Override
    public String getDsIndex() {
        return this.dsIndex;
    }

    @Override
    public void setStoreKey(Object storeKey) {
        this.storeKey = storeKey;
    }

    @Override
    public Object getStoreKey() {
        return this.storeKey;
    }
}
