package qunar.tc.qmq.common;

import com.google.common.base.Optional;

/**
 * @author yiqun.fan create on 17-8-23.
 */
public class AtomicStringConfig extends AtomicConfig<String> {
    @Override
    public Optional<String> parse(String key, String value) {
        return Optional.of(value);
    }
}
