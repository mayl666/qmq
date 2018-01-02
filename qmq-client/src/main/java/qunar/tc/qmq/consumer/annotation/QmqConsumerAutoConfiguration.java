package qunar.tc.qmq.consumer.annotation;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

import static qunar.tc.qmq.consumer.annotation.QmqClientBeanDefinitionParser.DEFAULT_ID;
import static qunar.tc.qmq.consumer.annotation.QmqClientBeanDefinitionParser.QMQ_CLIENT_ANNOTATION;

/**
 * Created by zhaohui.yu
 * 2/4/17
 */
@Configuration
@Import(QmqConsumerAutoConfiguration.Register.class)
class QmqConsumerAutoConfiguration {

    static class Register implements ImportBeanDefinitionRegistrar {

        @Override
        public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
            if (!registry.containsBeanDefinition(DEFAULT_ID)) {
                GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
                beanDefinition.setBeanClass(MessageConsumerProvider.class);
                beanDefinition.setRole(2);
                beanDefinition.setSynthetic(true);
                beanDefinition.setLazyInit(true);
                registry.registerBeanDefinition(DEFAULT_ID, beanDefinition);
            }

            if (!registry.containsBeanDefinition(QMQ_CLIENT_ANNOTATION)) {
                GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
                beanDefinition.setBeanClass(ConsumerAnnotationScanner.class);
                beanDefinition.setRole(2);
                beanDefinition.setSynthetic(true);
                registry.registerBeanDefinition(QMQ_CLIENT_ANNOTATION, beanDefinition);
            }
        }
    }
}
