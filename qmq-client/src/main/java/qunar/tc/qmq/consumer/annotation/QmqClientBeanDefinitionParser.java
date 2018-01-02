package qunar.tc.qmq.consumer.annotation;

import com.google.common.base.Strings;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

/**
 * User: yee.wang
 * Date: 2014/09/15
 * Time: 6:23 PM
 */
class QmqClientBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {
    static final String QMQ_CLIENT_ANNOTATION = "QMQ_CLIENT_ANNOTATION";

    static final String DEFAULT_ID = "QMQ_CONSUMER_ALL";

    @Override
    protected Class<?> getBeanClass(Element element) {
        return MessageConsumerProvider.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        String port = element.getAttribute("port");
        if (!Strings.isNullOrEmpty(port)) {
            builder.addPropertyValue("port", port);
        }

        if (!parserContext.getRegistry().containsBeanDefinition(QMQ_CLIENT_ANNOTATION)) {
            RootBeanDefinition annotation = new RootBeanDefinition(ConsumerAnnotationScanner.class);
            parserContext.getRegistry().registerBeanDefinition(QMQ_CLIENT_ANNOTATION, annotation);
        }
    }

    @Override
    protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext) {
        return DEFAULT_ID;
    }

}
