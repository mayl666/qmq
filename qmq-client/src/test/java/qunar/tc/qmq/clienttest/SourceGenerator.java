package qunar.tc.qmq.clienttest;


import com.google.common.base.Charsets;
import com.google.common.io.CharSink;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author keli.wang
 * @since 2017/9/8
 */
public class SourceGenerator {
    public static void main(String[] args) throws IOException {
        final CharSink targetSink = Files.asCharSink(new File("/tmp/source.txt"), Charsets.UTF_8, FileWriteMode.APPEND);
        final AtomicInteger counter = new AtomicInteger(0);

        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(7);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);

        for (int i = 0; i < 10_000; i++) {
            final List<String> batch = new ArrayList<>();
            for (int j = 0; j < 1000; j++) {
                batch.add(nf.format(counter.getAndIncrement()) + " " + UUID.randomUUID().toString());
            }
            targetSink.writeLines(batch);
        }
    }
}
