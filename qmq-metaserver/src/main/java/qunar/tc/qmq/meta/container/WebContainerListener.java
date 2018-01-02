package qunar.tc.qmq.meta.container;


import qunar.tc.qmq.meta.startup.ServerWrapper;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * User: zhaohuiyu Date: 1/7/13 Time: 4:35 PM
 */
public class WebContainerListener implements ServletContextListener {
    private ServerWrapper wrapper;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        wrapper = new ServerWrapper();
        wrapper.start();
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        if (this.wrapper != null) {
            wrapper.destroy();
        }
    }
}
