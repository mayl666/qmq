package qunar.tc.qmq.consumer.register;

import com.google.common.base.Strings;
import qunar.tc.qconfig.client.Feature;
import qunar.tc.qconfig.client.MapConfig;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.config.QConfigConstant;
import qunar.tc.qmq.consumer.pull.PullRegister;
import qunar.tc.qmq.producer.RegistryResolver;
import qunar.tc.qmq.service.ConsumerMessageHandler;
import qunar.tc.qmq.utils.Applications;

import java.util.*;

/**
 * User: zhaohuiyu
 * Date: 10/31/14
 * Time: 4:16 PM
 */
public class QConfigConsumerRegister implements ConsumerRegister {

    private final Set<String> registries;
    private final List<ConsumerRegister> registers;
    private final String host;
    private final Applications applications = Applications.getInstance();

    private ConsumerMessageHandler handler;
    private int port;
    private String version;
    private String subjectRoot;
    private String brokerGroupRoot;
    private String registry;

    public QConfigConsumerRegister(String host, RegistryResolver registryResolver) {
        this.host = host;
        this.registries = new HashSet<>();
        this.registers = new ArrayList<>();
        List<String> registries = resolveRegistries();
        for (String broker : registries) {
            String registry = registryResolver.resolve(broker);
            this.registries.add(registry);
        }
    }

    public QConfigConsumerRegister(String host, String registry) {
        this.host = host;
        this.registries = new HashSet<>();
        this.registers = new ArrayList<>();

        this.registry = registry;
        this.registries.add(registry);
    }

    public void init() {
        for (String registry : registries) {
            SubscribeInfo info = new SubscribeInfo(registry, host, port, subjectRoot, applications.getAppCode(), version);
            registers.add(new DirectConsumerRegister(brokerGroupRoot, info, handler));
            registers.add(new ZKConsumerRegister(info));
        }
        registers.add(new PullRegister());
    }

    @Override
    public void regist(String prefix, String group, RegistParam param) {
        for (ConsumerRegister register : registers) {
            register.regist(prefix, group, param);
        }
    }

    @Override
    public void unregist(String prefix, String group) {
        for (ConsumerRegister register : registers) {
            register.unregist(prefix, group);
        }
    }

    @Override
    public String registry() {
        return registry;
    }

    @Override
    public boolean offline() {
        for (ConsumerRegister register : registers) {
            register.offline();
        }
        return true;
    }

    @Override
    public boolean online() {
        for (ConsumerRegister register : registers) {
            register.online();
        }
        return true;
    }

    public void setAutoOnline(boolean autoOnline) {
        for (ConsumerRegister register : registers) {
            register.setAutoOnline(autoOnline);
        }
    }

    public void destroy() {
        for (ConsumerRegister register : registers) {
            register.destroy();
        }
    }

    public Set<String> getRegistries() {
        return Collections.unmodifiableSet(registries);
    }

    public void setSubjectRoot(String subjectRoot) {
        this.subjectRoot = subjectRoot;
    }

    public void setBrokerGroupRoot(String brokerGroupRoot) {
        this.brokerGroupRoot = brokerGroupRoot;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setConsumerMessageHandler(ConsumerMessageHandler handler) {
        this.handler = handler;
    }

    private List<String> resolveRegistries() {
        Map<String, String> map = MapConfig.get(QConfigConstant.QMQ_CLINET_GROUP, QConfigConstant.CONSUMER_REGISTRY, Feature.DEFAULT).asMap();

        //for application
        String brokers = map.get(applications.getAppCode());

        //for application.room
        if (Strings.isNullOrEmpty(brokers)) {
            brokers = map.get(applications.getAppCode() + '.' + applications.getRoom());
        }

        //for room
        if (Strings.isNullOrEmpty(brokers)) {
            brokers = map.get(applications.getRoom());
        }

        //default
        if (Strings.isNullOrEmpty(brokers)) {
            brokers = map.get(QConfigConstant.DEFAULT);
        }

        if (Strings.isNullOrEmpty(brokers)) {
            throw new RuntimeException("请联系TCDEV, consumer路由配置错误");
        }

        String[] list = brokers.split(",");
        return Arrays.asList(list);
    }

}
