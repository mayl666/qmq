package qunar.tc.qmq.utils;

import io.netty.buffer.ByteBuf;

import java.util.Map;

/**
 * @author yiqun.fan create on 17-8-2.
 */
public final class PayloadHolderUtils {

    public static void writeString(String s, ByteBuf out) {
        byte[] bs = CharsetUtils.toUTF8Bytes(s);
        out.writeShort((short) bs.length);
        out.writeBytes(bs);
    }

    public static String readString(ByteBuf in) {
        int len = in.readShort();
        byte[] bs = new byte[len];
        in.readBytes(bs);
        return CharsetUtils.toUTF8String(bs);
    }

    public static void writeBytes(byte[] bs, ByteBuf out) {
        out.writeInt(bs.length);
        out.writeBytes(bs);
    }

    public static byte[] readBytes(ByteBuf in) {
        int len = in.readInt();
        byte[] bs = new byte[len];
        in.readBytes(bs);
        return bs;
    }

    public static void writeStringMap(Map<String, String> map, ByteBuf out) {
        if (map == null || map.isEmpty()) {
            out.writeShort(0);
        } else {
            if (map.size() > Short.MAX_VALUE) {
                throw new IndexOutOfBoundsException("map is too large. size=" + map.size());
            }
            out.writeShort(map.size());
            for (Map.Entry<String, String> entry : map.entrySet()) {
                writeString(entry.getKey(), out);
                writeString(entry.getValue(), out);
            }
        }
    }

    public static Map<String, String> readStringMap(ByteBuf in, Map<String, String> map) {
        short size = in.readShort();
        for (int i = 0; i < size; i += 2) {
            map.put(readString(in), readString(in));
        }
        return map;
    }
}
