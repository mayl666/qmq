package qunar.tc.qmq.producer;

import java.util.Set;

/**
 * User: zhaohuiyu
 * Date: 10/28/14
 * Time: 2:30 PM
 */
public interface RegistryResolver {
    String resolve();

    String resolve(String dataCenter);

    String dataCenter();

    Set<String> list();
}
