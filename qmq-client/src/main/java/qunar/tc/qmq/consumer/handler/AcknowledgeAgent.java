/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.consumer.handler;

import com.alibaba.dubbo.config.ReferenceConfig;
import com.google.common.base.Preconditions;
import com.google.common.cache.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.agile.Numbers;
import qunar.tc.qmq.base.ACKMessage;
import qunar.tc.qmq.batch.BatchExecutor;
import qunar.tc.qmq.batch.Processor;
import qunar.tc.qmq.service.AcknowledgeService;
import qunar.tc.qmq.utils.PropertiesLoader;
import qunar.tc.qmq.utils.ReferenceBuilder;
import qunar.tc.qmq.utils.URLUtils;
import qunar.tc.qtracer.Constants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-28
 */
class AcknowledgeAgent implements Processor<ACKMessage> {
    private static final Logger log = LoggerFactory.getLogger(AcknowledgeAgent.class);

    private static final int ACK_THREADS;
    private static final int ACK_BATCH;

    private static final int MAX_QUEUE_SIZE;

    static {
        Properties parent = System.getProperties();
        Properties bundle = PropertiesLoader.load("qmq-consumer.properties", parent);

        if (bundle == null) {
            bundle = new Properties(parent); // 不直接使用parent, 是为了避免后期改动是无意中修改了全局属性.
            log.debug("没有找到qmq-consumer.properties, 采用默认设置");
        }

        ACK_THREADS = Numbers.toInt(bundle.getProperty("ack.threads"), 2);
        ACK_BATCH = Numbers.toInt(bundle.getProperty("ack.batch"), 20);
        MAX_QUEUE_SIZE = Numbers.toInt(bundle.getProperty("ack.queue.size"), 100000);
    }

    private static final LoadingCache<String, AcknowledgeAgent> cache = CacheBuilder.newBuilder().removalListener(new RemovalListener<String, AcknowledgeAgent>() {
        @Override
        public void onRemoval(RemovalNotification<String, AcknowledgeAgent> notification) {
            log.info("Remove ReferenceConfig {} from cache", notification.getKey());
            AcknowledgeAgent value = notification.getValue();
            if (value != null) {
                value.destroy();
            }
        }
    }).build(new CacheLoader<String, AcknowledgeAgent>() {
        @Override
        public AcknowledgeAgent load(String key) {
            log.info("Build ReferenceConfig {} to cache", key);
            // key: zookeeper://127.0.0.1:20081?group={broker group}&backup=10.20.153.11:2181,10.20.153.12:2181
            ReferenceConfig<AcknowledgeService> ref = ReferenceBuilder
                    .newRef(AcknowledgeService.class)
                    .withRegistryAddress(URLUtils.filterFile(key))
                    .build();
            ref.setCheck(Boolean.FALSE);
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(Constants.DUBBO_TRACE_SWITCH_KEY, "false");
            ref.setParameters(parameters);
            ref.get();
            return new AcknowledgeAgent(ref, ACK_THREADS, ACK_BATCH, MAX_QUEUE_SIZE);
        }
    });

    public static void acknowledge(String registry, ACKMessage msg) throws ExecutionException {
        cache.get(registry).add(msg);
    }

    private final ReferenceConfig<AcknowledgeService> ackService;

    private final BatchExecutor<ACKMessage> executor;

    private AcknowledgeAgent(ReferenceConfig<AcknowledgeService> ackService, int ackThreads, int batch, int maxQueueSize) {
        this.ackService = ackService;
        executor = new BatchExecutor<ACKMessage>("ack-agent", batch, this);
        executor.setQueueSize(maxQueueSize);
        executor.setThreads(ackThreads);
        executor.init();
    }

    public void add(ACKMessage msg) {
        executor.addItem(msg);
    }

    public void destroy() {
        executor.destroy();
        ackService.destroy();
    }

    @Override
    public void process(List<ACKMessage> ackMessages) {
        Preconditions.checkNotNull(ackService);
        try {
            ackService.get().acknowledge(ackMessages);
        } catch (Exception e) {
            log.error("Send ack error", e);
        }
    }
}
