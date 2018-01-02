package qunar.tc.qmq.consumer.annotation;

import com.alibaba.dubbo.common.compiler.Compiler;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import org.springframework.aop.support.AopUtils;
import qunar.tc.qmq.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: zhaohuiyu
 * Date: 9/16/14
 * Time: 3:16 PM
 */
class GeneratedListener implements MessageListener, FilterAttachable, IdempotentAttachable {

    private static final AtomicInteger index = new AtomicInteger();

    private static final Compiler compiler = ExtensionLoader.getExtensionLoader(Compiler.class).getDefaultExtension();

    private final MessageListener listener;
    private final IdempotentChecker idempotentChecker;
    private final List<Filter> filters;

    GeneratedListener(Object bean, Method method, IdempotentChecker idempotentChecker, List<Filter> filters) {
        this.idempotentChecker = idempotentChecker;
        this.filters = filters;
        int current = index.incrementAndGet();
        String generateListenerClassName = "Listener" + current;
        ClassLoader classLoader = bean.getClass().getClassLoader();
        StringBuilder code = new StringBuilder();
        code.append("public class ").append(generateListenerClassName).append(" implements qunar.tc.qmq.MessageListener{\n");
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        String fullName = targetClass.getCanonicalName();
        code.append("private ").append(fullName).append(" target=null;\n");
        code.append("public ").append(generateListenerClassName).append("(").append(fullName).append(" target){this.target=target;}\n");
        code.append("public void onMessage(qunar.tc.qmq.Message message){this.target.").append(method.getName()).append("(message);}\n}");
        Class<?> listenerClass = compiler.compile(code.toString(), classLoader);

        Constructor<?> constructor = null;
        try {
            constructor = listenerClass.getConstructor(targetClass);
            this.listener = (MessageListener) constructor.newInstance(bean);
        } catch (Throwable e) {
            throw new RuntimeException("init generate listener error");
        }
    }

    @Override
    public void onMessage(Message msg) {
        this.listener.onMessage(msg);
    }

    @Override
    public List<Filter> filters() {
        return filters;
    }

    @Override
    public IdempotentChecker getIdempotentChecker() {
        return idempotentChecker;
    }
}
