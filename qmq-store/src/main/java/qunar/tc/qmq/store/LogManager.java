package qunar.tc.qmq.store;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author keli.wang
 * @since 2017/7/3
 */
public class LogManager {
    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);

    private final File logDir;
    private final int fileSize;

    private final MessageStoreConfig config;

    private final LogSegmentValidator segmentValidator;
    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();

    private long flushedOffset = 0;

    public LogManager(final File dir, final int fileSize, final MessageStoreConfig config, final LogSegmentValidator segmentValidator) {
        this.logDir = dir;
        this.fileSize = fileSize;
        this.config = config;
        this.segmentValidator = segmentValidator;
        createAndValidateLogDir();
        loadLogs();
        recover();
    }

    // ensure dir ok
    private void createAndValidateLogDir() {
        if (!logDir.exists()) {
            LOG.info("Log directory {} not found, try create it.", logDir.getAbsoluteFile());
            final boolean created = logDir.mkdirs();
            if (!created) {
                throw new RuntimeException("Failed to create log directory " + logDir.getAbsolutePath());
            }
        }

        if (!logDir.isDirectory() || !logDir.canRead()) {
            throw new RuntimeException(logDir.getAbsolutePath() + " is not a readable log directory");
        }
    }

    private void loadLogs() {
        LOG.info("Loading logs.");
        final File[] files = logDir.listFiles();
        if (files != null) {
            for (final File file : files) {
                if (file.getName().startsWith(".")) {
                    continue;
                }
                try {
                    final LogSegment segment = new LogSegment(file, fileSize);
                    segment.setWrotePosition(fileSize);
                    segment.setFlushedPosition(fileSize);
                    segments.put(segment.getBaseOffset(), segment);
                    LOG.info("Load {} success.", file.getAbsolutePath());
                } catch (IOException e) {
                    LOG.error("Load {} failed.", file.getAbsolutePath());
                }
            }
        }
        LOG.info("Load logs done.");
    }

    private void recover() {
        if (segments.isEmpty()) {
            return;
        }

        LOG.info("Recovering logs.");
        final List<Long> baseOffsets = new ArrayList<>(segments.navigableKeySet());
        final int offsetCount = baseOffsets.size();
        long offset = -1;
        for (int i = offsetCount - 2; i < offsetCount; i++) {
            if (i < 0) {
                continue;
            }

            final LogSegment segment = segments.get(baseOffsets.get(i));
            offset = segment.getBaseOffset();

            final LogSegmentValidator.ValidateResult result = segmentValidator.validate(segment);
            offset += result.getValidatedSize();
            if (result.getStatus() == LogSegmentValidator.ValidateStatus.COMPLETE) {
                segment.setWrotePosition(segment.getFileSize());
                segment.setFlushedPosition(segment.getFileSize());
            } else {
                break;
            }
        }
        flushedOffset = offset;

        final long maxOffset = latestSegment().getBaseOffset() + latestSegment().getFileSize();
        final int relativeOffset = (int) (offset % fileSize);
        final LogSegment segment = locateSegment(offset);
        if (segment != null && maxOffset != offset) {
            segment.setWrotePosition(relativeOffset);
            segment.setFlushedPosition(relativeOffset);
            LOG.info("recover wrote offset to {}:{}", segment, segment.getWrotePosition());
            // TODO(keli.wang): should delete crash file
        }
        LOG.info("Recover done.");
    }

    public LogSegment locateSegment(final long offset) {
        if (isBaseOffset(offset)) {
            return segments.get(offset);
        }

        final Map.Entry<Long, LogSegment> entry = segments.lowerEntry(offset);
        if (entry == null) {
            return null;
        } else {
            return entry.getValue();
        }
    }

    private boolean isBaseOffset(final long offset) {
        return offset % fileSize == 0;
    }

    public LogSegment firstSegment() {
        final Map.Entry<Long, LogSegment> entry = segments.firstEntry();
        return entry == null ? null : entry.getValue();
    }

    public LogSegment latestSegment() {
        final Map.Entry<Long, LogSegment> entry = segments.lastEntry();
        return entry == null ? null : entry.getValue();
    }

    public LogSegment allocNextSegment() {
        final long nextBaseOffset = nextSegmentBaseOffset();
        return allocSegment(nextBaseOffset);
    }

    private long nextSegmentBaseOffset() {
        final LogSegment segment = latestSegment();
        if (segment == null) {
            return 0;
        } else {
            return segment.getBaseOffset() + fileSize;
        }
    }

    private LogSegment allocSegment(final long baseOffset) {
        final File nextSegmentFile = new File(logDir, StoreUtils.offset2FileName(baseOffset));
        try {
            final LogSegment segment = new LogSegment(nextSegmentFile, fileSize);
            segments.put(baseOffset, segment);
            LOG.info("alloc new segment file {}", segment);
            return segment;
        } catch (IOException e) {
            LOG.error("Failed create new segment file. file: {}", nextSegmentFile.getAbsolutePath());
        }
        return null;
    }

    public LogSegment allocOrResetSegments(final long expectedOffset) {
        final long baseOffset = computeBaseOffset(expectedOffset);

        if (segments.isEmpty()) {
            return allocSegment(baseOffset);
        }

        if (nextSegmentBaseOffset() == baseOffset && latestSegment().isFull()) {
            return allocSegment(baseOffset);
        }

        LOG.warn("All segments are too old, need to delete all segment now. Current base offset: {}, expect base offset: {}",
                latestSegment().getBaseOffset(), baseOffset);
        deleteAllSegments();

        return allocSegment(baseOffset);
    }

    private long computeBaseOffset(final long offset) {
        return offset - (offset % fileSize);
    }

    private void deleteAllSegments() {
        for (final long key : segments.keySet()) {
            deleteSegment(key);
        }
    }

    public long getMinOffset() {
        final LogSegment segment = firstSegment();
        if (segment == null) {
            return 0;
        }
        return segment.getBaseOffset();
    }

    public long getMaxOffset() {
        final LogSegment segment = latestSegment();
        if (segment == null) {
            return 0;
        }
        return segment.getBaseOffset() + segment.getWrotePosition();
    }

    public boolean flush() {
        boolean result = true;
        final LogSegment segment = locateSegment(flushedOffset);
        if (segment != null) {
            final int offset = segment.flush();
            final long where = segment.getBaseOffset() + offset;
            result = where == this.flushedOffset;
            this.flushedOffset = where;
        }
        return result;
    }

    public void close() {
        for (final LogSegment segment : segments.values()) {
            segment.close();
        }
    }

    public void deleteExpiredSegments(final long retentionMs) {
        final long deleteUntil = System.currentTimeMillis() - retentionMs;
        Preconditions.checkState(deleteUntil > 0, "retentionMs不应该超过当前时间");

        if (segments.size() <= 1) {
            return;
        }

        for (final Map.Entry<Long, LogSegment> entry : segments.entrySet()) {
            final long key = entry.getKey();
            final LogSegment segment = entry.getValue();

            if (segment.getLastModifiedTime() < deleteUntil) {
                if (!config.isDeleteExpiredLogsEnable()) {
                    LOG.info("should delete expired segment {}, but delete expired logs is disabled for now", segment);
                    continue;
                }

                if (deleteSegment(key)) {
                    LOG.info("remove expired segment success. segment: {}", segment);
                } else {
                    LOG.warn("remove expired segment failed. segment: {}", segment);
                }
            }
        }
    }

    private boolean deleteSegment(final long key) {
        final LogSegment segment = segments.remove(key);
        return segment.destroy();
    }
}
