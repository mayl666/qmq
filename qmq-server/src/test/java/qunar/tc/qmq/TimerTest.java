package qunar.tc.qmq;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author yunfeng.yang
 * @since 2017/7/10
 */
public class TimerTest {

    public static void main(String[] args) throws Exception {
        //创建Timer, 精度为100毫秒,
        HashedWheelTimer timer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS, 16);

        System.out.println(new Date(System.currentTimeMillis()));

        timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                System.out.println(new Date(System.currentTimeMillis()));
                System.out.println(timeout);
            }
        }, 5, TimeUnit.SECONDS);

        //阻塞main线程
        System.in.read();
    }

}
