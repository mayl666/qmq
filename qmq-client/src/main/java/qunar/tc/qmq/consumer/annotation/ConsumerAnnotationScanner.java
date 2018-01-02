package qunar.tc.qmq.consumer.annotation;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.AbstractBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import qunar.tc.qmq.Filter;
import qunar.tc.qmq.IdempotentChecker;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageListener;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static qunar.tc.qmq.consumer.annotation.QmqClientBeanDefinitionParser.DEFAULT_ID;


/**
 * User: yee.wang
 * Date:2014/09/16
 * Time: 10:20 am
 * 不考虑多线程并发加载context的情况
 */
class ConsumerAnnotationScanner implements BeanPostProcessor, ApplicationContextAware, BeanFactoryAware {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerAnnotationScanner.class);

    private static final Set<Method> registeredMethods = new HashSet<Method>();

    private ApplicationContext context;

    private AbstractBeanFactory beanFactory;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        parseMethods(bean, bean.getClass().getDeclaredMethods());
        return bean;
    }

    private void parseMethods(final Object bean, Method[] methods) {
        String beanName = bean.getClass().getCanonicalName();

        for (final Method method : methods) {
            if (registeredMethods.contains(method)) continue;

            QmqConsumer annotation = AnnotationUtils.findAnnotation(method, QmqConsumer.class);
            if (annotation == null) continue;

            if (!Modifier.isPublic(method.getModifiers())) {
                throw new RuntimeException("标记QmqConsumer的方法必须是public的");
            }

            String methodName = method.getName();
            Class[] args = method.getParameterTypes();
            String message = String.format("如果想配置成为message listener,方法必须有且只有一个参数,类型必须为qunar.tc.qmq.Message类型: %s method:%s", beanName, methodName);
            if (args.length != 1) {
                logger.error(message);
                throw new RuntimeException(message);
            }
            if (args[0] != Message.class) {
                logger.error(message);
                throw new RuntimeException(message);
            }

            String prefix = resolve(annotation.prefix());

            if (Strings.isNullOrEmpty(prefix)) {
                String err = String.format("使用@QmqConsumer,必须提供prefix, class:%s method:%s", beanName, methodName);
                logger.error(err);
                throw new RuntimeException(err);
            }

            registeredMethods.add(method);

            String consumerGroup = annotation.isBroadcast() ? "" : resolve(annotation.consumerGroup());
            ListenerHolder listenerHolder = new ListenerHolder(context, beanFactory, bean, method, prefix, consumerGroup, annotation.idempotentChecker(), annotation.filters());
            listenerHolder.registe();
        }
    }

    private String resolve(String value) {
        if (Strings.isNullOrEmpty(value)) return value;
        if (beanFactory == null) return value;
        return beanFactory.resolveEmbeddedValue(value);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        if (beanFactory instanceof AbstractBeanFactory) {
            this.beanFactory = (AbstractBeanFactory) beanFactory;
        }
    }

    private static class ListenerHolder {
        private final ApplicationContext context;
        private final MessageListener listener;
        private final AbstractBeanFactory beanFactory;
        private final String prefix;
        private final String group;

        private ListenerHolder(ApplicationContext context,
                               AbstractBeanFactory beanFactory,
                               Object bean,
                               Method method,
                               String prefix,
                               String group,
                               String idempotentChecker,
                               String[] filters) {
            this.context = context;
            this.beanFactory = beanFactory;
            this.prefix = prefix;
            this.group = group;
            IdempotentChecker idempotentCheckerBean = null;
            if (idempotentChecker != null && idempotentChecker.length() > 0) {
                idempotentCheckerBean = context.getBean(idempotentChecker, IdempotentChecker.class);
            }

            List<Filter> filterBeans = new ArrayList<>();
            if (filters != null && filters.length > 0) {
                for (int i = 0; i < filters.length; ++i) {
                    filterBeans.add(context.getBean(filters[i], Filter.class));
                }
            }
            this.listener = new GeneratedListener(bean, method, idempotentCheckerBean, filterBeans);
        }

        public void registe() {
            MessageConsumerProvider consumer = resolveConsumer();
            consumer.addListener(prefix, group, listener);
        }

        private MessageConsumerProvider resolveConsumer() {
            MessageConsumerProvider result = null;

            if (beanFactory != null) {
                result = beanFactory.getBean(DEFAULT_ID, MessageConsumerProvider.class);
            }
            if (result != null) return result;

            if (context != null) {
                result = context.getBean(DEFAULT_ID, MessageConsumerProvider.class);
            }
            if (result != null) return result;

            throw new RuntimeException("没有正确的配置qmq，如果使用Springboot请确保升级到了最新版本");
        }
    }
}
