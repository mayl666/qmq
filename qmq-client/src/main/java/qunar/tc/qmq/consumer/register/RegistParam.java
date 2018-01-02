package qunar.tc.qmq.consumer.register;

import qunar.tc.qmq.MessageListener;

import java.util.concurrent.Executor;

/**
 * @author yiqun.fan create on 17-8-23.
 */
public class RegistParam {
    private final ExecutorConfig executorConfig;
    private final MessageListener messageListener;
    private final boolean isBroadcast;

    public RegistParam(ExecutorConfig executorConfig, MessageListener messageListener, boolean isBroadcast) {
        this.executorConfig = executorConfig;
        this.messageListener = messageListener;
        this.isBroadcast = isBroadcast;
    }

    public ExecutorConfig getExecutorConfig() {
        return executorConfig;
    }

    public Executor getExecutor() {
        return executorConfig.getExecutor();
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public boolean isBroadcast() {
        return isBroadcast;
    }
}
