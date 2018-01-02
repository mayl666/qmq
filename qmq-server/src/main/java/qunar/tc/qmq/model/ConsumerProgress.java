package qunar.tc.qmq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yunfeng.yang
 * @since 2017/7/6
 */
public class ConsumerProgress {
    @JsonIgnore
    private final AtomicInteger index = new AtomicInteger(0);
    private ConcurrentMap<String, ConsumerLogOffset> subjects;

    @JsonCreator
    public ConsumerProgress(@JsonProperty("subjects") ConcurrentMap<String, ConsumerLogOffset> subjects) {
        this.subjects = subjects;
    }

    public ConcurrentMap<String, ConsumerLogOffset> getSubjects() {
        return subjects;
    }

    @Override
    public String toString() {
        return "ConsumerProgress{" +
                "index=" + index +
                ", subjects=" + subjects +
                '}';
    }
}
