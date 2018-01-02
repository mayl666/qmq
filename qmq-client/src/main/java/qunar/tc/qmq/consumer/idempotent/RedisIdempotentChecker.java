package qunar.tc.qmq.consumer.idempotent;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import qunar.tc.qclient.redis.RedisAsyncClient;
import qunar.tc.qclient.redis.config.RedisConfig;
import qunar.tc.qclient.redis.factory.RedisAsyncClientFactoryBean;
import qunar.tc.qmq.Message;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaohui.yu
 * 15/11/4
 * <p/>
 * 使用redis实现的幂等检查
 * 设置了过期时间，默认过期时间1天
 */
public class RedisIdempotentChecker extends AbstractIdempotentChecker {

    private long expire = TimeUnit.DAYS.toSeconds(1);

    private final RedisAsyncClient redisClient;

    private static final RedisConfig config = RedisConfig.newBuilder().withMaxCommandSize(1024 * 1024 * 5).withEncodeInEventLoop(true).withDiscardPolicy(RedisConfig.DiscardPolicy.DROP).build();

    public RedisIdempotentChecker(String namespace, String password) {
        this(namespace, password, DEFAULT_KEYFUNC);
    }

    public RedisIdempotentChecker(String namespace, String password, Function<Message, String> keyFunc) {
        super(keyFunc);
        try {
            RedisAsyncClientFactoryBean factoryBean = new RedisAsyncClientFactoryBean(namespace);
            factoryBean.setConfig(config);
            factoryBean.setCipher(password);
            this.redisClient = factoryBean.create();
        } catch (RuntimeException e) {
            throw new RuntimeException("无法连接你所配置的redis集群");
        }
    }

    @Override
    protected boolean doIsProcessed(Message message) throws Exception {
        String key = keyOf(message);
        ListenableFuture<Boolean> result = redisClient.setnx(key, 1);
        redisClient.expire(key, expire);
        if (result.get()) return false;
        return true;
    }

    @Override
    protected void markFailed(Message message) {
        redisClient.del(keyOf(message));
    }

    @Override
    protected void markProcessed(Message message) {

    }

    @Override
    public void garbageCollect(Date before) {

    }

    public void setExpire(long period, TimeUnit unit) {
        this.expire = unit.toSeconds(period);
    }
}
