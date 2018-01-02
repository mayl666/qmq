package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class ActionLogIterateService {
    private static final Logger LOG = LoggerFactory.getLogger(ActionLogIterateService.class);

    private final ActionLog log;
    private final ActionLogDispatchService dispatchService;
    private final Thread dispatcherThread;

    private volatile boolean stop = false;
    private volatile long iterateFrom;

    public ActionLogIterateService(final ActionLog log, final long iterateFrom, final ActionLogDispatchService dispatchService) {
        this.log = log;
        this.dispatcherThread = new Thread(new Dispatcher());
        this.iterateFrom = iterateFrom;
        this.dispatchService = dispatchService;
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
                final ActionLogVisitor visitor = log.newVisitor(iterateFrom);
                while (true) {
                    final Optional<Action> action = visitor.nextAction();
                    if (action == null) {
                        break;
                    }

                    if (!action.isPresent()) {
                        continue;
                    }

                    dispatchService.dispatch(action.get());
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
