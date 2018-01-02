package qunar.tc.qmq.web;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.codec.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.metrics.Counter;
import qunar.metrics.Metrics;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.web.service.ConsumerTestService;
import qunar.tc.qmq.web.service.impl.ConsumerTestServiceImpl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author yunfeng.yang
 * @since 2017/7/12
 */
public class ConsumerServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerServlet.class);

    private static final Counter PULL_SUCCESS = Metrics.counter("ConsumerTest.PullSuccess").delta().get();
    private static final Counter CONSUME_ERROR = Metrics.counter("ConsumerTest.ConsumeERROR").delta().get();

    private ConsumerTestService consumerTestService = new ConsumerTestServiceImpl();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String action = req.getParameter("action");
        String prefix = req.getParameter("prefix");
        String group = req.getParameter("group");
        final boolean needError = req.getParameter("error") == null;

        if ("stop".equals(action)) {
            consumerTestService.stopListener(prefix, group);
            logger.info("stop consumer ok, {}, {}", prefix, group);
            resp.getWriter().write("stop ok");
            return;
        }

        consumerTestService.addListener(prefix, group, new MessageListener() {

            @Override
            public void onMessage(Message msg) {
                final int randomNum = ThreadLocalRandom.current().nextInt(0, 10);
//                logger.info("msg : {}", JacksonSupport.toJson(msg));
                final String data = msg.getStringProperty("data");
                final int oldCode = Integer.parseInt(msg.getStringProperty("code"));
                final HashFunction hash = Hashing.sha1();
                final HashCode code = hash.hashString(data, Charsets.UTF_8);
                boolean result = oldCode == code.asInt();
                if (!result) {
                    CONSUME_ERROR.inc();
                }
                if (needError && randomNum == 5) {
                    throw new RuntimeException("err test");
                }
//                logger.info("result : {}, length :{}", msg.getBooleanProperty("result"), msg.getDateProperty("today"));
//                logger.info("oldCode:{}, code:{}, equal:{}", oldCode, code.asInt(), result);
                PULL_SUCCESS.inc();
            }
        });

        logger.info("start consumer ok, {}, {}", prefix, group);
        resp.getWriter().write("start ok");
    }
}
