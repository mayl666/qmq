package qunar.tc.qmq.base;

import java.util.List;

/**
 * @author yunfeng.yang
 * @since 2017/9/1
 */
public class BrokerRegisterResponse {
    private List<String> subjects;

    public List<String> getSubjects() {
        return subjects;
    }

    public void setSubjects(List<String> subjects) {
        this.subjects = subjects;
    }
}
