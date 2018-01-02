package qunar.tc.qmq.protocol;

/**
 * @author yunfeng.yang
 * @since 2017/6/30
 */
public interface CommandCode {

    // response code
    short SUCCESS = 0;
    short UNKNOWN_CODE = 3;
    short NO_MESSAGE = 4;

    short BROKER_ERROR = 51;
    short BROKER_REJECT = 52;

    // request code
    short SEND_MESSAGE = 10;
    short PULL_MESSAGE = 11;
    short ACK_REQUEST = 12;
    short SYNC_REQUEST = 20;

    short BROKER_REGISTER = 30;
    short CLIENT_REGISTER = 35;

    // heartbeat
    short HEARTBEAT = 100;

}
