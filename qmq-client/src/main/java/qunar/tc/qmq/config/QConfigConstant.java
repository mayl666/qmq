package qunar.tc.qmq.config;

/**
 * Created by yee.wang on 2014/9/17.
 */
public class QConfigConstant {
    public static final String SPLITER = "\\|";

    //从这里拉取公共的zookeeper配置(必须)
    public static final String PUBLIC_CONFIG = "tc_public_config";
    public static final String REGISTRY_CONFIG_FILE = "registry.properties";

    //client配置
    public static final String QMQ_CLINET_GROUP = "tc_qmq_client";

    //for producer
    //现在线上的broker集群(必须)
    public static final String BROKER_CONFIG = "broker.properties";
    //给各个subject可以定义路由，路由到哪个broker集群(可选)
    public static final String ROUTER_ZK = "router.properties";

    //for consumer，consumer的线程池大小和队列大小配置
    //全局和每个业务都可以设置
    public static final String SHARD_EXECUTOR_CONFIG = "quota.properties";

    public static final String PULL_SUBJECTS_CONFIG = "pull_subject_config.properties";
    public static final String META_IPSERVER_CONFIG = "meta_ipserver_config.properties";

    //向哪些机房consumer订阅
    public static final String CONSUMER_CONFIG = "consumer.properties";

    public static final String CONSUMER_REGISTRY = "consumer_registry.properties";

    public static final String DEFAULT = "default";

    public static final String DIRECT_CONSUMER_SWITCH = "direct_consumer_switch.properties";

    public static final String ROUTER_DISPATCH = "router_dispatch.t";
}
