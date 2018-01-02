package qunar.tc.qmq.store;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2017/8/23
 */
public class MessageLogIterateService {
    private static final Logger LOG = LoggerFactory.getLogger(MessageLogIterateService.class);

    private final MessageLog log;
    private final EventBus messageMetaDispatcher;
    private final Thread dispatcherThread;

    private volatile boolean stop = false;
    private volatile long iterateFrom;

    public MessageLogIterateService(final MessageLog log, final long iterateFrom, final EventBus messageMetaDispatcher) {
        this.log = log;
        this.dispatcherThread = new Thread(new Dispatcher());
        this.iterateFrom = iterateFrom;
        this.messageMetaDispatcher = messageMetaDispatcher;
    }

    public void start() {
        dispatcherThread.start();
    }

    public void shutdown() {
        stop = true;
        try {
            dispatcherThread.join();
        } catch (InterruptedException e) {
            LOG.error("action log dispatcher thread interrupted", e);
        }
    }

    private class Dispatcher implements Runnable {
        @Override
        public void run() {
            while (!stop) {
                final MessageLogMetaVisitor visitor = log.newVisitor(iterateFrom);
                while (true) {
                    final Optional<MessageLogMeta> meta = visitor.nextRecord();
                    if (meta == null) {
                        break;
                    }

                    if (!meta.isPresent()) {
                        continue;
                    }

                    messageMetaDispatcher.post(meta.get());
                }
                iterateFrom += visitor.visitedBufferSize();

                try {
                    TimeUnit.MILLISECONDS.sleep(5);
                } catch (InterruptedException e) {
                    LOG.warn("action log dispatcher sleep interrupted");
                }
            }
        }
    }
}
