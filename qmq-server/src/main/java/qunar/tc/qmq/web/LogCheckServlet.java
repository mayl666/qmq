package qunar.tc.qmq.web;

import qunar.tc.qmq.web.service.LogCheckService;
import qunar.tc.qmq.web.service.impl.LogCheckServiceImpl;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author yunfeng.yang
 * @since 2017/7/25
 */
public class LogCheckServlet extends HttpServlet {
    private LogCheckService logCheckService = new LogCheckServiceImpl();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String action = req.getParameter("action");
        String produceLogPath = req.getParameter("produceLogPath");
        String outputPath = req.getParameter("outputPath");

        if ("compareMessageLog".equals(action)) {
            logCheckService.compareProduceAndLog(produceLogPath, outputPath);
        } else if ("compareConsume".equals(action)) {
            String consumeLogPath = req.getParameter("consumeLogPath");
            logCheckService.compareProduceAndConsume(produceLogPath, consumeLogPath, outputPath);
        }
    }
}
