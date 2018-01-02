/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.base;

import java.io.Serializable;

/** 
 * @author miao.yang susing@gmail.com
 * @date 2012-12-26
 */
public class ChannelState implements Serializable{

	private static final long serialVersionUID = 5116114634898930399L;

	private final long currentTime;
	
	private final int activeCount;
	
	public ChannelState(int activeCount, long currentTime){
		this.activeCount = activeCount;
		this.currentTime = currentTime;
	}
	
	/**
	 * @return 返回当时这个通道正在执行的任务总数.
	 */
	public int getActiveCount(){
		return activeCount;
	}
	
	/**
	 * @return 返回consumer生成状态快照的时间点.
	 */
	public long getTime(){
		return currentTime;
	}
}
