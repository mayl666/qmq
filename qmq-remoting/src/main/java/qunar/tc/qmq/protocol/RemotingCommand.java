package qunar.tc.qmq.protocol;

/**
 * @author yunfeng.yang
 * @since 2017/7/3
 */
public class RemotingCommand extends Datagram {

    public RemotingCommandType getCommandType() {
        int bits = 1;
        int flag0 = this.header.getFlag() & bits;
        return RemotingCommandType.codeOf(flag0);
    }

    public boolean isOneWay() {
        int bits = 1 << 1;
        return (this.header.getFlag() & bits) == bits;
    }

    public int getOpaque() {
        return header.getOpaque();
    }

}
