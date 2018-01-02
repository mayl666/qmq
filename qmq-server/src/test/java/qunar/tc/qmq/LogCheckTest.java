package qunar.tc.qmq;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author yunfeng.yang
 * @since 2017/7/25
 */
public class LogCheckTest {
    private static final String PRODUCE_LOG_PATH = "/Users/yangyunfeng/IdeaProjects/newqmq/qmq-server/qmq.2017-07-25.log";
    private static final Pattern PRODUCE_PATTERN = Pattern.compile("^.{10}\\p{Blank}.{9}\\p{Blank}\\[(.*):(.*)]\\t发送成功.$");

    public static void main(String[] args) throws IOException {
        File file = new File(PRODUCE_LOG_PATH);
        List<String> lines = Files.readLines(file, Charsets.UTF_8, new LineProcessor<List<String>>() {
            final List<String> result = Lists.newArrayList();

            @Override
            public boolean processLine(String line) throws IOException {
                Matcher produceMatcher = PRODUCE_PATTERN.matcher(line);
//                if (line.contains("发送成功")) {
//                    System.out.println(line);
//                    return false;
//                }
                if (!produceMatcher.matches()) {
                    return true;
                }

                result.add(line);
                return true;
            }

            @Override
            public List<String> getResult() {
                return result;
            }
        });
        System.out.println(lines.size());

        String log = "2017-07-25 16:41:46: [170725.164146.10.90.7.155.25586.0:new.qmq.test]\t发送成功.";

    }
}
