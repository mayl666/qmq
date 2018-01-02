/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.producer.sender;

import com.alibaba.dubbo.config.ReferenceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.Corporation;
import qunar.management.ServerManager;
import qunar.tc.qmq.base.JdbcInfo;
import qunar.tc.qmq.config.QConfigRegistryResolver;
import qunar.tc.qmq.SqlConstant;
import qunar.tc.qmq.service.DataSourceTrackService;
import qunar.tc.qmq.utils.Constants;
import qunar.tc.qmq.utils.ReferenceBuilder;
import qunar.tc.qmq.utils.URLUtils;

import java.util.List;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-8
 */
public class PositionSpy implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(PositionSpy.class);
    private static String appCode;

    static {
        try {
            appCode = ServerManager.getInstance().getAppConfig().getName();
        } catch (Throwable e) {
            log.warn("获取应用名称失败,将使用空应用名", e);
            appCode = "";
        }
    }

    public static void notify(List<String> keys) {
        Corporation corporation = ServerManager.getInstance().getCorporation();
        if (corporation == Corporation.CTRIP) {
            return;
        }

        String registry = QConfigRegistryResolver.INSTANCE.resolve();

        StringBuilder result = new StringBuilder();
        result.append("*******************向qmq汇报的数据源如下所示***********************\n");
        for (String key : keys) {
            result.append(key);
            result.append("\n");
        }
        result.append("**********************************************************");
        log.info(result.toString());

        String group = String.format("%s/%s", Constants.TASK_GROUP_PATH, "default");
        String registryURL = URLUtils.buildZKUrl(registry, group);
        PositionSpy spy = new PositionSpy(registryURL, keys);

        Thread t = new Thread(spy, "position-spy");
        t.setDaemon(true);
        t.start();
    }

    private final String registry;
    private final List<String> keys;

    public PositionSpy(String registry, List<String> keys) {
        this.registry = registry;
        this.keys = keys;
    }

    @Override
    public void run() {
        ReferenceConfig<DataSourceTrackService> tracker = null;

        try {
            tracker = ReferenceBuilder
                    .newRef(DataSourceTrackService.class)
                    .withRegistryAddress(registry)
                    .build();
            tracker.setCheck(false);
            for (String key : keys) {
                JdbcInfo info = new JdbcInfo(key, SqlConstant.TABLE_NAME, appCode);
                tracker.get().track(info);
            }
        } catch (Exception e) {
            log.error("DataSourceTrackService 无法注册跟踪本地数据库信息.", e);
        } finally {
            if (tracker != null)
                tracker.destroy();
        }
    }
}
