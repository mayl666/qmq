package qunar.tc.qmq.util;

import qunar.tc.qmq.protocol.Datagram;
import qunar.tc.qmq.protocol.PayloadHolder;
import qunar.tc.qmq.protocol.RemotingCommandType;
import qunar.tc.qmq.protocol.RemotingHeader;

/**
 * @author yunfeng.yang
 * @since 2017/7/6
 */
public class RemotingBuilder {

    public static Datagram buildResponseDatagram(short code, int opaque, PayloadHolder payloadHolder) {
        Datagram datagram = new Datagram();
        datagram.setHeader(buildResponseHeader(code, opaque));
        datagram.setPayloadHolder(payloadHolder);
        return datagram;
    }

    public static Datagram buildRequestDatagram(short code, PayloadHolder payloadHolder) {
        Datagram datagram = new Datagram();
        datagram.setHeader(buildRemotingHeader(code, RemotingCommandType.REQUEST_COMMAND.getCode(), 0));
        datagram.setPayloadHolder(payloadHolder);
        return datagram;
    }

    public static RemotingHeader buildResponseHeader(short code, int opaque) {
        return buildRemotingHeader(code, RemotingCommandType.RESPONSE_COMMAND.getCode(), opaque);
    }

    private static RemotingHeader buildRemotingHeader(short code, int flag, int opaque) {
        RemotingHeader remotingHeader = new RemotingHeader();
        remotingHeader.setCode(code);
        remotingHeader.setFlag(flag);
        remotingHeader.setOpaque(opaque);
        return remotingHeader;
    }
}
