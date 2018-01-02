package qunar.tc.qmq.producer.sender;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.List;
import java.util.Map;

/**
 * @author zhenyu.nie created on 2017 2017/7/3 12:48
 */
class NopRoute implements Route {

    private static final Logger logger = LoggerFactory.getLogger(NopRoute.class);

    public static final Connection NOP_CONNECTION = new Connection() {

        @Override
        public void preHeat() {

        }

        @Override
        public String url() {
            return "";
        }

        @Override
        public Map<String, MessageException> send(List<ProduceMessage> messages) {
            logger.warn("send message to nop route, {}", messages);
            return ImmutableMap.of();
        }

        @Override
        public void destroy() {

        }
    };

    @Override
    public boolean isNewRoute() {
        return false;
    }

    @Override
    public void preHeat() {

    }

    @Override
    public Connection route() {
        return NOP_CONNECTION;
    }
}
