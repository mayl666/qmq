package qunar.tc.qmq;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-26
 */
public interface Message {
    String getMessageId();

    String getSubject();

    Date getCreatedTime();

    Date getExpiredTime();

    Date getScheduleReceiveTime();

    void setProperty(String name, boolean value);

    void setProperty(String name, int value);

    void setProperty(String name, long value);

    void setProperty(String name, float value);

    void setProperty(String name, double value);

    void setProperty(String name, Date date);

    void setProperty(String name, String value);

    Object getProperty(String name);

    String getStringProperty(String name);

    boolean getBooleanProperty(String name);

    Date getDateProperty(String name);

    @Deprecated
    Map<String, Object> getAttrs();

    /**
     * 不推荐使用该方法，推荐将对象预先系列化好json，然后使用setStringProperty设置
     *
     * @param data
     */
    @Deprecated
    void setData(Object data);

    /**
     * 不推荐使用该方法，推荐使用getStringProperty获取json字符串，然后自行解析
     *
     * @param clazz
     * @param <T>
     * @return
     */
    @Deprecated
    <T> T getData(Class<T> clazz);

    void setReliabilityLevel(ReliabilityLevel level);

    ReliabilityLevel getReliabilityLevel();

    /**
     * 期望显式的手动ack时，使用该方法关闭qmq默认的自动ack。
     * 该方法必须是在consumer端的MessageListener的onMessage方法入口处调用，否则会抛出异常
     * <p/>
     * 在producer端调用时会抛出UnsupportedOperationException异常
     *
     * @param auto
     */
    void autoAck(boolean auto);

    /**
     * 显式手动ack的时候，使用该方法
     *
     * @param elapsed 消息处理时长
     * @param e       如果消息处理失败请传入异常，否则传null
     *                <p/>
     *                在producer端调用会抛出UnsupportedOperationException异常
     */
    void ack(long elapsed, Throwable e);

    void setDelayTime(Date date);

    void setDelayTime(long delayTime, TimeUnit timeUnit);

    /**
     * 第几次发送
     * 使用方应该监控该次数，如果不是刻意设计该次数不应该太多
     *
     * @return
     */
    int times();

    void setMaxRetryNum(int maxRetryNum);

    int getMaxRetryNum();

    /**
     * 本地连续重试次数
     *
     * @return
     */
    int localRetries();

    void setDurable(boolean durable);

    boolean isDurable();

    void setStoreAtFailed(boolean storeAtFailed);

    boolean isStoreAtFailed();
}
