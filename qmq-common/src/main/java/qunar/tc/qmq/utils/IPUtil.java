package qunar.tc.qmq.utils;

import com.alibaba.dubbo.common.utils.NetUtils;
import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * User: zhaohuiyu
 * Date: 1/6/13
 * Time: 4:00 PM
 */
public class IPUtil {
    private static final Logger logger = LoggerFactory.getLogger(IPUtil.class);

    public static String getLocalHost(String zkAddress) {
        String result = null;
        try {
            result = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("get local result error", e);

        }
        if (NetUtils.isInvalidLocalHost(result)) {
            String[] addressPair = parseZKHostAndPort(zkAddress);
            Socket socket = null;
            try {
                socket = new Socket();
                SocketAddress remoteAddr = new InetSocketAddress(addressPair[0], Integer.parseInt(addressPair[1]));
                socket.connect(remoteAddr, 100);
                result = socket.getLocalAddress().getHostAddress();
            } catch (Exception e) {
                logger.error("get result error", e);
            } finally {
                try {
                    if (socket != null) {
                        socket.close();
                    }
                } catch (Exception e) {
                    logger.debug("close socket failed.", e);
                }
            }
            if (NetUtils.isInvalidLocalHost(result)) {
                result = NetUtils.getLocalHost();
            }
        }
        return result;
    }

    public static String getLocalHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("get local result error", e);
            return "";
        }
    }

    public static String getLocalHostName() {
        try {
            // TODO: 2017/8/9 hostname
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.warn("get local result error", e);
            return "";
        }
    }

    private static String[] parseZKHostAndPort(String zkAddress) {
        try {
            Iterable<String> split = Splitter.on(",").split(zkAddress);
            Iterator<String> iterator = Splitter.on(":").split(split.iterator().next()).iterator();
            return new String[]{iterator.next(), iterator.next()};
        } catch (NoSuchElementException e) {
            String message = "请确认你的zkAddress配置是否正确，你的zkAddress地址为: " + zkAddress + ". 请参考 http://wiki.corp.qunar.com/pages/viewpage.action?pageId=63243065 页底部zookeeper地址一节";
            throw new RuntimeException(message);
        }
    }
}