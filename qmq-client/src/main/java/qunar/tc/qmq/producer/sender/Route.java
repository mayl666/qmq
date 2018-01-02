package qunar.tc.qmq.producer.sender;

/**
 * @author zhenyu.nie created on 2017 2017/7/3 12:24
 */
public interface Route {

    boolean isNewRoute();

    void preHeat();

    Connection route();
}
