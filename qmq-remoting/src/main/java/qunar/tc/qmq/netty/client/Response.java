package qunar.tc.qmq.netty.client;

/**
 * @author yiqun.fan create on 17-8-29.
 */
public interface Response {
    int getStatusCode();

    String getHeader(String name);

    String getBody();
}
