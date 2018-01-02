package qunar.tc.qmq;

/**
 * Created by zhaohui.yu
 * 10/31/16
 */
public interface ProduceMessage {

    String getMessageId();

    String getSubject();

    void save();

    void send();

    void error(Exception e);

    void failed();

    void block();

    void finish();

    void setStoreKey(Object storeKey);

    Object getStoreKey();

    void setDsIndex(String dsIndex);

    String getDsIndex();

    Message getBase();

    void setStore(MessageStore store);
}
