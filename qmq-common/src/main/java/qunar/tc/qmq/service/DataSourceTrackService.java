/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.service;

import qunar.tc.qmq.base.JdbcInfo;

/** 
 * @author miao.yang susing@gmail.com
 * @date 2013-1-7
 */
public interface DataSourceTrackService {

	
	void track(JdbcInfo info);
	
}
