package qunar.tc.qmq.consumer.annotation;

import java.lang.annotation.*;

/**
 * User: zhaohuiyu
 * Date: 7/5/13
 * Time: 7:28 PM
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface QmqConsumer {
    /**
     * (必填)订阅的prefix, 支持${prefix}形式注入
     */
    String prefix();

    /**
     * (可选)consumerGroup,如果不填则为广播模式, 支持${group}形式注入
     */
    String consumerGroup() default "";

    /**
     * 是否是广播消息
     * 设成true时, 忽略consumerGroup的值
     */
    boolean isBroadcast() default false;

    /**
     * 幂等检查器beanName
     */
    String idempotentChecker() default "";

    /**
     * 过滤器beanName，按顺序
     */
    String[] filters() default {};
}

