package qunar.tc.qmq.producer.idgenerator;

import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.NetUtils;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: zhaohuiyu
 * Date: 6/4/13
 * Time: 10:06 AM
 */
public class TimestampAndHostIdGenerator implements IdGenerator {
    private static final int[] codex = {2, 3, 5, 6, 8, 9, 19, 11, 12, 14, 15, 17, 18};
    private static final AtomicInteger messageOrder = new AtomicInteger(0);

    private static final String localAddress = NetUtils.getLocalHost();

    //在生成message id的时候带上进程id，避免一台机器上部署多个服务都发同样的消息时出问题
    private static final int PID = ConfigUtils.getPid();

    @Override
    public String getNext() {
        StringBuilder sb = new StringBuilder(40);
        long time = System.currentTimeMillis();
        String ts = new Timestamp(time).toString();

        for (int idx : codex)
            sb.append(ts.charAt(idx));
        sb.append('.').append(localAddress);
        sb.append('.').append(PID);
        sb.append('.').append(messageOrder.getAndIncrement()); //可能为负数.但是无所谓.
        return sb.toString();
    }
}
