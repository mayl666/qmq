package qunar.tc.qmq.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.web.service.ProducerTestService;
import qunar.tc.qmq.web.service.impl.ProducerTestServiceImpl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author yunfeng.yang
 * @since 2017/7/11
 */
public class ProducerServlet extends HttpServlet {
    private static final Logger logger = LoggerFactory.getLogger(ProducerServlet.class);

    private ProducerTestService producerTestService = new ProducerTestServiceImpl();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String action = req.getParameter("action");
        if ("stop".equals(action)) {
            producerTestService.stop();
            logger.info("stop producer ok");
            resp.getWriter().write("stop ok");
            return;
        }
        String qps = req.getParameter("qps");
        String subject = req.getParameter("subject");
        String len = req.getParameter("len");
        boolean isLow = req.getParameter("low") != null;
        producerTestService.start(Double.valueOf(qps), subject, Integer.valueOf(len), isLow);
        logger.info("stop producer ok, {}, {}, {}", qps, subject, len);
        resp.getWriter().write("start ok");
    }
}
