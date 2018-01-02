/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.consumer;

import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.ServiceConfig;
import com.alibaba.dubbo.remoting.transport.dispather.direct.DirectDispather;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import qunar.management.ServerManager;
import qunar.tc.qmq.ListenerHolder;
import qunar.tc.qmq.MessageConsumer;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.RejectPolicy;
import qunar.tc.qmq.config.QConfigRegistryResolver;
import qunar.tc.qmq.consumer.handler.MessageDistributor;
import qunar.tc.qmq.consumer.register.QConfigConsumerRegister;
import qunar.tc.qmq.producer.RegistryResolver;
import qunar.tc.qmq.service.ConsumerMessageHandler;
import qunar.tc.qmq.utils.Constants;
import qunar.tc.qmq.utils.IPUtil;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-28
 */
public class MessageConsumerProvider implements MessageConsumer, InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumerProvider.class);

    /**
     * 当consumer的api更新时，请更新该版本号
     * <p/>
     * 升级版本号时注意兼容性
     */
    private static final String DEFAULT_VERSION = "1.2.0";

    private String version = DEFAULT_VERSION;

    private static final int MAX_CONSUMER_GROUP_LEN = 50;
    private static final int MAX_PREFIX_LEN = 100;

    private String brokerGroupRoot = Constants.BROKER_GROUP_ROOT;

    private String subjectRoot;

    private int port;

    private static final int timeout = 3000;

    private MessageDistributor distributor;

    private final QConfigConsumerRegister register;

    private String me;

    private QConfigSharedExecutor executors;

    private boolean useRandomPort = false;

    private final AtomicBoolean STARTED = new AtomicBoolean(false);

    /**
     * 端口使用随机分配的，zk订阅所有集群的
     * <p/>
     * 只供<qmq:consumer />这种形式使用
     */
    public MessageConsumerProvider() {
        this.port = NetUtils.getAvailablePort();
        this.useRandomPort = true;
        this.executors = new QConfigSharedExecutor();
        RegistryResolver registryResolver = QConfigRegistryResolver.INSTANCE;
        this.me = IPUtil.getLocalHost(registryResolver.resolve());
        this.register = new QConfigConsumerRegister(me, registryResolver);
        log.warn("友情提示: 如果使用QMQ 2.0,并且使用广播的模式(addListener第二个参数为空),则你需要调用给MessageConsumerProvider设置端口号或者<qmq:consumer port=\"30000\" />");
    }

    /**
     * 为了兼容保留，端口已经是自动探测的，对于没有防火墙的应用无须指定
     *
     * @param zkAddress
     */
    @Deprecated
    public MessageConsumerProvider(String zkAddress) {
        this(zkAddress, 30000);
    }

    public MessageConsumerProvider(String zkAddress, int servicePort) {
        Preconditions.checkArgument(servicePort > 0, "端口非法");
        Preconditions.checkArgument(!StringUtils.isEmpty(zkAddress), "zk 地址为空，请检查配置");

        this.me = IPUtil.getLocalHost(zkAddress);
        this.port = servicePort;
        this.register = new QConfigConsumerRegister(me, zkAddress);
        log.warn("友情提示: 你使用了QMQ 2.0，但没有使用2.0的配置方式，使用QMQ 2.0将获得更好的可靠性支持，在多个机房任意漂移，并且配置更简单，参考: http://wiki.corp.qunar.com/pages/viewpage.action?pageId=64114065");
    }

    @Deprecated
    public void init() {
        if (STARTED.compareAndSet(false, true)) {
            log.info("配置的注册中心地址为: {}, 探测到你的消费者地址是: {}:{}, 请检查是否正确", register.getRegistries(), me, port);

            if (executors != null) {
                executors.init();
            }

            register.setSubjectRoot(subjectRoot);
            register.setBrokerGroupRoot(brokerGroupRoot);
            register.setVersion(version);
            register.setPort(port);

            distributor = new MessageDistributor(register);
            distributor.setMyAddress(me + ":" + port);
            distributor.setBrokerGroupRoot(brokerGroupRoot);

            register.setConsumerMessageHandler(distributor);
            register.init();

            export();

            registeSwitchable();
        }
    }

    QConfigConsumerRegister getRegister() {
        return register;
    }

    /**
     * 判断一下业务是否开启了根据healthcheck.html的上下线机制
     * 如果开启了，就由这种上下线机制来控制qmq上下线
     * 否则就一订阅就立即上线了
     */
    private void registeSwitchable() {
        boolean isAutoOnline;
        try {
            Field field = ServerManager.class.getDeclaredField("isSetupHealthCheckPoller");
            field.setAccessible(true);
            isAutoOnline = !(Boolean) field.get(ServerManager.getInstance());
        } catch (NoSuchFieldException e) {
            String message = "从qmq 1.3.5 版本开始启用了根据healthcheck.html自动上下线机制，该机制最低依赖common包8.1.8版本，关于更多细节可以参考wiki: http://wiki.corp.qunar.com/pages/viewpage.action?pageId=63243158 或咨询TCDev";
            log.error(message, e);
            isAutoOnline = true;
        } catch (IllegalAccessException e) {
            isAutoOnline = true;
        }

        if (!isAutoOnline) {
            log.info("开启了qmq根据healthcheck.html上下线机制，如果你发现无法收到消息，并且启动无任何异常，请检查webapps/ROOT下是否有healthcheck.html文件");
            ServerManager.getInstance().addSwitcher("qmq:" + this.port, register);
        } else {
            log.warn("从 qmq 1.3.5 版本开始启用了根据healthcheck.html自动上下线机制，该机制需要配置qunar.ServletWatcher listener才能生效(并且作为第一个listener)，现检测未配置该listener，将会关闭该机制。");
            register.setAutoOnline(isAutoOnline);
        }
    }

    private void export() {
        ApplicationConfig appConfig = new ApplicationConfig("QmQ");

        RegistryConfig registryConfig = new RegistryConfig("N/A");

        ProtocolConfig protocolConfig = new ProtocolConfig();
        protocolConfig.setPort(port);

        protocolConfig.setDispather(DirectDispather.NAME);

        ServiceConfig<ConsumerMessageHandler> messageService = new ServiceConfig<ConsumerMessageHandler>();
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("qloglevel", "2");
        parameters.put(qunar.tc.qtracer.Constants.DUBBO_TRACE_SWITCH_KEY, "false");
        messageService.setParameters(parameters);
        messageService.setCluster("failfast");
        messageService.setApplication(appConfig);
        messageService.setRegistry(registryConfig);
        messageService.setProtocol(protocolConfig);
        messageService.setInterface(ConsumerMessageHandler.class);
        messageService.setVersion(version);
        messageService.setTimeout(timeout);
        messageService.setRef(distributor);
        messageService.export();
    }

    public ListenerHolder addListener(String subjectPrefix, String consumerGroup, MessageListener listener) {
        if (executors == null) {
            throw new RuntimeException("只有使用QMQ 2.0的配置模式才能使用不需要传递线程池的方法: http://wiki.corp.qunar.com/pages/viewpage.action?pageId=64114065");
        }
        //subjectPrefix=1|2|3
        Executor executor = executors.getExecutor(subjectPrefix);
        return add(subjectPrefix, consumerGroup, listener, executor, RejectPolicy.Abort);
    }

    @Override
    public ListenerHolder addListener(String subjectPrefix, String consumerGroup, MessageListener listener, ThreadPoolExecutor executor) {
        return add(subjectPrefix, consumerGroup, listener, executor, RejectPolicy.Abort);
    }

    @Override
    @Deprecated
    public ListenerHolder addListener(String subjectPrefix, String consumerGroup, MessageListener listener, ThreadPoolExecutor executor, RejectPolicy rejectPolicy) {
        return add(subjectPrefix, consumerGroup, listener, executor, rejectPolicy);
    }

    private ListenerHolder add(String subjectPrefix, String consumerGroup, MessageListener listener, Executor executor, RejectPolicy rejectPolicy) {
        if (!STARTED.get()) {
            throw new RuntimeException("请先调用MessageConsumerProvider的afterPropertiesSet()方法再注册监听!");
        }

        Preconditions.checkArgument(subjectPrefix != null && subjectPrefix.length() <= MAX_PREFIX_LEN, "subjectPrefix长度不允许超过" + MAX_PREFIX_LEN + "个字符");
        Preconditions.checkArgument(consumerGroup == null || consumerGroup.length() <= MAX_CONSUMER_GROUP_LEN, "consumerGroup长度不允许超过" + MAX_CONSUMER_GROUP_LEN + "个字符");

        Preconditions.checkArgument(!subjectPrefix.contains("${"), "请确保subjectPrefix已经正确解析: " + subjectPrefix);
        Preconditions.checkArgument(consumerGroup == null || !consumerGroup.contains("${"), "请确保consumerGroup已经正确解析: " + consumerGroup);

        if (Strings.isNullOrEmpty(consumerGroup) && useRandomPort) {
            throw new RuntimeException("使用广播的方式，请使用setPort方法显式设置监听的端口号");
        }

        return distributor.addListener(subjectPrefix, consumerGroup, listener, executor, rejectPolicy);
    }

    public void setSubjectRoot(String subjectRoot) {
        this.subjectRoot = subjectRoot;
    }

    public void setBrokerGroupRoot(String brokerGroupRoot) {
        if (!Strings.isNullOrEmpty(brokerGroupRoot))
            this.brokerGroupRoot = brokerGroupRoot;
    }

    /**
     * 如果机器上有多网卡可以设置这个
     *
     * @param address
     */
    public void setAddress(String address) {
        this.me = address;
    }

    /**
     * 设置版本号
     *
     * @param version
     */
    public void setVersion(String version) {
        this.version = version;
    }

    public void setPort(int port) {
        Preconditions.checkArgument(port > 0, "port端口非法");
        this.useRandomPort = false;
        this.port = port;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }

    @Override
    public void destroy() {
        register.destroy();
    }
}
