package qunar.tc.qmq.processor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import qunar.tc.qmq.base.BrokerRole;
import qunar.tc.qmq.base.RawMessage;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.configuration.Config;
import qunar.tc.qmq.netty.AsyncRequestContext;
import qunar.tc.qmq.netty.NettyRequestProcessor;
import qunar.tc.qmq.protocol.QMQSerializer;
import qunar.tc.qmq.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author yunfeng.yang
 * @since 2017/7/4
 */
public class SendMessageProcessor implements NettyRequestProcessor {
    private static final ExecutorService RECEIVE_POOL = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryBuilder().setNameFormat("receive-message-%d").build());
    private final Receiver receiver;
    private final Config config;

    public SendMessageProcessor(Receiver receiver, Config config) {
        this.receiver = receiver;
        this.config = config;
    }

    @Override
    public void processRequest(ChannelHandlerContext ctx, RemotingCommand command) throws Exception {
        final List<RawMessage> messages = QMQSerializer.deserializeRawMessages(command.getBody());
        if (config.getBoolean("receiver.flusher.enable", false)) {
            final AsyncRequestContext context = AsyncRequestContext.getContext();
            context.setFuture(receiver.receive(messages));
        } else {
            final AsyncRequestContext context = AsyncRequestContext.getContext();
            RECEIVE_POOL.submit(new ReceiveTask(receiver, messages, context));
        }
    }

    private static final class ReceiveTask implements Runnable {
        private final Receiver receiver;
        private final List<RawMessage> messages;
        private final AsyncRequestContext context;

        ReceiveTask(Receiver receiver, List<RawMessage> messages, AsyncRequestContext context) {
            this.receiver = receiver;
            this.messages = messages;
            this.context = context;
        }

        @Override
        public void run() {
            context.setFuture(receiver.receive(messages));
        }
    }

    @Override
    public boolean rejectRequest() {
        return BrokerConfig.getBrokerRole() == BrokerRole.SLAVE;
    }
}
