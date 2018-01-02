package qunar.tc.qmq;

/**
 * User: zhaohuiyu
 * Date: 8/1/13
 * Time: 2:18 PM
 */
public enum ReliabilityLevel {
    High,
    Middle,
    Low;

    public static boolean isLow(ReliabilityLevel level) {
        return Low.equals(level);
    }

    public static boolean isLow(Message message) {
        return Low.equals(message.getReliabilityLevel());
    }

    public static boolean isMiddle(ReliabilityLevel level) {
        return Middle.equals(level);
    }

    public static boolean isHigh(ReliabilityLevel level) {
        return High.equals(level);
    }

    public static boolean isMiddle(Message message) {
        return Middle.equals(message.getReliabilityLevel());
    }

    public static boolean isHigh(Message message) {
        return High.equals(message.getReliabilityLevel());
    }
}
