package qunar.tc.qmq.meta;

import qunar.tc.qmq.utils.IPUtil;

/**
 * @author yunfeng.yang
 * @since 2017/9/11
 */
public class IpUtilTest {
    public static void main(String[] args) {
        String localHost = IPUtil.getLocalHost();
        System.out.println(localHost);
    }
}
