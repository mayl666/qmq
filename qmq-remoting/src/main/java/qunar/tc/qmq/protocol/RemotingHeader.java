package qunar.tc.qmq.protocol;

import qunar.api.pojo.node.JacksonSupport;

/**
 * @author yiqun.fan create on 17-7-4.
 */
public class RemotingHeader {
    public static final int DEFAULT_MAGIC_CODE = 0xdec1_0ade;

    public static final short VERSION_1 = 1;

    public static final short MIN_HEADER_SIZE = 16;
    public static final short HEADER_SIZE_LEN = 2;
    public static final short TOTAL_SIZE_LEN = 4;

    public static final short LENGTH_FIELD = TOTAL_SIZE_LEN + HEADER_SIZE_LEN;

    private int magicCode = DEFAULT_MAGIC_CODE;
    private short code;
    private short version = VERSION_1;
    private int opaque;
    private int flag;

    public int getMagicCode() {
        return magicCode;
    }

    public void setMagicCode(int magicCode) {
        this.magicCode = magicCode;
    }

    public short getCode() {
        return code;
    }

    public void setCode(short code) {
        this.code = code;
    }

    public short getVersion() {
        return version;
    }

    public void setVersion(short version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return JacksonSupport.toJson(this);
    }

}
