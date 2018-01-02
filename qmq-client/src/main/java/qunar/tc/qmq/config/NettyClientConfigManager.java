package qunar.tc.qmq.config;

import qunar.tc.qmq.netty.NettyClientConfig;

/**
 * @author yiqun.fan create on 17-7-4.
 */
public class NettyClientConfigManager {
    private static final NettyClientConfigManager config = new NettyClientConfigManager();

    public static NettyClientConfigManager get() {
        return config;
    }

    private volatile NettyClientConfig clientConfig = new NettyClientConfig();

    private NettyClientConfigManager() {
    }

    public NettyClientConfig getDefaultClientConfig() {
        return clientConfig;
    }
}
