/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq;


import qunar.Corporation;
import qunar.management.ServerManager;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-9
 */
public class SqlConstant {

    public static final String DATABASE_NAME = "qmq_produce";

    public static final String TABLE_NAME;

    public static final String insertSQL;
    public static final String errorSQL;
    public static final String finishSQL;

    static {
        Corporation corporation = ServerManager.getInstance().getCorporation();
        TABLE_NAME = corporation == Corporation.QUNAR ? DATABASE_NAME + "." + "qmq_msg_queue" : "qmq_msg_queue";
        insertSQL = String.format("INSERT INTO %s (message_id,create_time,content) VALUES(?,?,?)", TABLE_NAME);
        errorSQL = String.format("UPDATE %s SET status=?,error=error+1,update_time=? WHERE message_id=?", TABLE_NAME);
        finishSQL = String.format("DELETE FROM %s WHERE message_id=?", TABLE_NAME);
    }

}
