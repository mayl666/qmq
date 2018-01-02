/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.base;

import java.io.Serializable;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-7
 */
public class JdbcInfo implements Serializable {

    private static final long serialVersionUID = 4290953780961057863L;

    private final String metaURL;
    private final String tableName;
    private final String appCode;

    public JdbcInfo(String metaURL, String tableName, String appCode) {
        this.metaURL = metaURL;
        this.tableName = tableName;
        this.appCode = appCode;
    }

    public String getAppCode() {
        return appCode;
    }

    public String getMetaURL() {
        return metaURL;
    }

    public String getTableName() {
        return tableName;
    }

}
