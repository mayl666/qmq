package qunar.tc.qmq.web.service;

import java.io.IOException;

/**
 * @author yunfeng.yang
 * @since 2017/7/25
 */
public interface LogCheckService {

    void compareProduceAndLog(String produceLogPath, String outputPath) throws IOException;

    void compareProduceAndConsume(String produceLogPath, String consumeLogPath, String outputPath) throws IOException;
}
