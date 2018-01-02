package qunar.tc.qmq.consumer.annotation;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * User: zhaohuiyu
 * Date: 7/5/13
 * Time: 5:37 PM
 */
class QmqClientNamespaceHandler extends NamespaceHandlerSupport {
    @Override
    public void init() {
        registerBeanDefinitionParser("consumer", new QmqClientBeanDefinitionParser());
    }
}
