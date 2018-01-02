/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.base;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.ReliabilityLevel;
import qunar.tc.qmq.serializer.Serializer;
import qunar.tc.qmq.serializer.SerializerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-26
 */
public class BaseMessage implements Message, Serializable {
    private static final long serialVersionUID = 303069262539600333L;

    private static final int MAX_MESSAGE_ID_LEN = 100;
    public static final int DEFAULT_MAX_RETRY_NUM = 16;

    String messageId;

    String subject;

    private boolean durable = true;

    private transient boolean storeAtFailed = false;

    private transient Serializer serializer;

    public enum keys {
        qmq_createTIme,
        qmq_expireTime,
        qmq_consumerGroupName,
        qmq_prefix,
        qmq_registry,
        qmq_brokerGroupName,
        qmq_data,
        qmq_reliabilityLevel,
        qmq_scheduleRecevieTime,
        qmq_scheduleDelay,
        qmq_receiver,
        qmq_times,
        qmq_maxRetryNum,
        qmq_refCnt,
        qmq_receiverHost,
        qmq_appCode,
        qmq_traceId,
        qmq_spanId,
        qmq_delayIndex,
        qmq_size,
        qmq_traceContext,
        qmq_tryNext,
        qmq_pullOffset,
        qmq_consumerOffset,
    }

    private static final Set<String> keyNames = Sets.newHashSet();

    static {
        for (keys key : keys.values())
            keyNames.add(key.name());
    }

    HashMap<String, Object> attrs = new HashMap<>();

    public BaseMessage() {
    }

    public BaseMessage(String messageId, String subject) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(messageId), "message id should not empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(subject), "message subject should not empty");
        Preconditions.checkArgument(messageId.length() <= MAX_MESSAGE_ID_LEN, "messageId长度不能超过" + MAX_MESSAGE_ID_LEN + "个字符");
        Preconditions.checkArgument(subject.length() <= MAX_MESSAGE_ID_LEN, "subject的长度不能超过" + MAX_MESSAGE_ID_LEN + "个字符");

        this.messageId = messageId;
        this.subject = subject;
        long time = System.currentTimeMillis();
        setProperty(keys.qmq_createTIme, time);
        setProperty(keys.qmq_maxRetryNum, DEFAULT_MAX_RETRY_NUM);
    }

    public BaseMessage(BaseMessage message) {
        this(message.getMessageId(), message.getSubject());
        this.durable = message.durable;
        this.serializer = message.serializer;
        attrs = new HashMap<>(message.attrs);
    }

    public Map<String, Object> getAttrs() {
        return Collections.unmodifiableMap(attrs);
    }

    @Deprecated
    public void setAttrs(HashMap<String, Object> attrs) {
        this.attrs = attrs;
    }

    @Override
    public String getMessageId() {
        return messageId;
    }

    @Override
    public String getSubject() {
        return subject;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    @Override
    @JsonIgnore
    public Date getCreatedTime() {
        return getDateProperty(keys.qmq_createTIme.name());
    }

    public void setProperty(keys key, boolean value) {
        attrs.put(key.name(), Boolean.valueOf(value));
    }

    public void setProperty(keys key, String value) {
        attrs.put(key.name(), value);
    }

    public void setProperty(keys key, int value) {
        attrs.put(key.name(), value);
    }

    public void setProperty(keys key, long value) {
        attrs.put(key.name(), value);
    }

    public void setProperty(keys key, Date value) {
        attrs.put(key.name(), value.getTime());
    }

    /**
     * 为了类型属性的稳定此方法一定不能暴漏成public.
     */
    private void setObjectProperty(String name, Object value) {
        if (keyNames.contains(name))
            throw new IllegalArgumentException("property name [" + name + "] is protected. ");
        attrs.put(name, value);
    }

    @Override
    public void setProperty(String name, boolean value) {
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, int value) {
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, long value) {
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, float value) {
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, double value) {
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, Date value) {
        setObjectProperty(name, value.getTime());
    }

    @Override
    public void setProperty(String name, String value) {
        setObjectProperty(name, value);
    }

    @Override
    public Object getProperty(String name) {
        return attrs.get(name);
    }

    @Override
    public String getStringProperty(String name) {
        return valueOfString(attrs.get(name));
    }

    @Override
    public boolean getBooleanProperty(String name) {
        Object v = attrs.get(name);
        if (v == null)
            return false;
        return Boolean.valueOf(v.toString());
    }

    @Override
    public Date getDateProperty(String name) {
        Object o = attrs.get(name);
        if (o == null)
            return null;
        Long v = Long.valueOf(o.toString());
        return new Date(v);
    }

    private static String valueOfString(Object str) {
        return str == null ? null : str.toString();
    }

    public Object getProperty(keys key) {
        return getProperty(key.name());
    }

    public String getStringProperty(keys key) {
        return getStringProperty(key.name());
    }

    public void removeProperty(keys key) {
        attrs.remove(key.name());
    }

    @Override
    public void setData(Object data) {
        Preconditions.checkNotNull(serializer, "请不要自行创建BaseMessage");
        attrs.put(keys.qmq_data.name(), serializer.serialize(data));
    }

    @Override
    public <T> T getData(Class<T> clazz) {
        Preconditions.checkNotNull(serializer, "请不要自行创建BaseMessage");

        Object json = attrs.get(keys.qmq_data.name());
        if (json == null)
            return null;
        return serializer.deSerialize(json.toString(), clazz);
    }

    @Override
    public void setReliabilityLevel(ReliabilityLevel level) {
        setProperty(keys.qmq_reliabilityLevel, level.name());
    }

    @Override
    @JsonIgnore
    public ReliabilityLevel getReliabilityLevel() {
        String level = getStringProperty(keys.qmq_reliabilityLevel);
        if (level == null || level.length() == 0)
            return ReliabilityLevel.High;
        return ReliabilityLevel.valueOf(level);
    }

    @Override
    public void autoAck(boolean auto) {
        throw new UnsupportedOperationException("请在consumer端设置auto ack");
    }

    @Override
    public void ack(long elapsed, Throwable e) {
        throw new UnsupportedOperationException("BaseMessage does not support this method");
    }

    @Override
    @JsonIgnore
    public Date getExpiredTime() {
        return getDateProperty(keys.qmq_expireTime.name());
    }

    public void setExpiredTime(long time) {
        setProperty(keys.qmq_expireTime, time);
    }

    public void setExpiredDelay(long timeDelay, TimeUnit timeUnit) {
        setExpiredTime(System.currentTimeMillis() + timeUnit.toMillis(timeDelay));
    }

    @Override
    public void setDelayTime(Date date) {
        Preconditions.checkNotNull(date, "消息定时接收时间不能为空");
        long time = date.getTime();
        Preconditions.checkArgument(time > System.currentTimeMillis(), "消息定时接收时间不能为过去时");

        setExpiredTime(time + getOriExpireTime());
        setDelay(time);
    }

    // WARNING setProperty(String
    // name,...)这个版本的方法里面会对name进行检查，如果这个name在keys集合(qmq内部使用)
    // 中则会抛出异常，这是为了防止业务使用到这些内部保留关键字。
    // 所以qmq内部使用的属性都应该使用setProperty(keys key,...)这个版本。
    private void setDelay(long time) {
        setProperty(keys.qmq_scheduleRecevieTime, time);
    }

    @Override
    public void setDelayTime(long delayTime, TimeUnit timeUnit) {
        Preconditions.checkNotNull(timeUnit, "消息延迟接收时间单位不能为空");
        Preconditions.checkArgument(delayTime >= 0, "消息延迟接收时间不能为过去时");

        long sendTime = System.currentTimeMillis() + timeUnit.toMillis(delayTime);
        setExpiredTime(sendTime + getOriExpireTime());
        setDelay(sendTime);
    }

    private long getOriExpireTime() {
        return getExpiredTime().getTime() - getCreatedTime().getTime();
    }

    @Override
    @JsonIgnore
    public Date getScheduleReceiveTime() {
        return getDateProperty(keys.qmq_scheduleRecevieTime.name());
    }

    @Override
    public int times() {
        Object o = getProperty(keys.qmq_times);
        if (o == null) return 0;
        return Integer.valueOf(o.toString());
    }

    @Override
    public void setMaxRetryNum(int maxRetryNum) {
        Preconditions.checkArgument(maxRetryNum >= 0, "maxRetryNum must >= 0");
        setProperty(keys.qmq_maxRetryNum, maxRetryNum);
    }

    @Override
    public int getMaxRetryNum() {
        String value = getStringProperty(keys.qmq_maxRetryNum);
        if (Strings.isNullOrEmpty(value)) {
            return -1;
        }
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return -1;
        }
    }

    @Override
    public int localRetries() {
        throw new UnsupportedOperationException("本地重试，只有消费端才支持");
    }

    @Override
    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    @Override
    public boolean isDurable() {
        return durable;
    }

    @Override
    public void setStoreAtFailed(boolean storeAtFailed) {
        this.storeAtFailed = storeAtFailed;
    }

    @Override
    public boolean isStoreAtFailed() {
        return this.storeAtFailed;
    }

    public void setSerializer(Serializer serializer) {
        Preconditions.checkNotNull(serializer, "serializer不能为null");

        this.serializer = serializer;
    }

    public String serializeTo() {
        Preconditions.checkNotNull(serializer, "请不要自行创建BaseMessage");

        return serializer.serialize(this);
    }

    @Override
    public String toString() {
        return SerializerFactory.create().serialize(this);
    }

}
