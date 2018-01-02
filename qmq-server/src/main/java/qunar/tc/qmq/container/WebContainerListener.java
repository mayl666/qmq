package qunar.tc.qmq.container;

import qunar.tc.qmq.configuration.AbstractConfig;
import qunar.tc.qmq.configuration.QConfig;
import qunar.tc.qmq.startup.ServerWrapper;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * User: zhaohuiyu Date: 1/7/13 Time: 4:35 PM
 */
public class WebContainerListener implements ServletContextListener {
    private ServerWrapper wrapper;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        AbstractConfig config = new QConfig();
        wrapper = new ServerWrapper(config);
        wrapper.start();
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        if (this.wrapper != null) {
            wrapper.destroy();
        }
    }
}
