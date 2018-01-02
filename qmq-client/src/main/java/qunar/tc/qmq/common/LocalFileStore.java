package qunar.tc.qmq.common;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.serializer.Serializer;
import qunar.tc.qmq.serializer.impl.JacksonSerializer;
import qunar.tc.qmq.utils.AppStoreUtil;

import java.io.File;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public abstract class LocalFileStore<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileStore.class);

    private static final Serializer DEFAULT_SERIALIZER = new JacksonSerializer();

    private final File file;
    private final Class clz;
    private final Serializer serializer;
    private volatile T value;

    protected LocalFileStore(String filename, Class clz) {
        this(filename, clz, DEFAULT_SERIALIZER);
    }

    protected LocalFileStore(String filename, Class clz, Serializer serializer) {
        this.file = new File(AppStoreUtil.getAppStore(), filename);
        this.clz = clz;
        this.serializer = serializer;
    }

    public Optional<T> load(boolean create) {
        if (value != null) {
            return Optional.of(value);
        }
        return reload(create);
    }

    public Optional<T> reload(boolean create) {
        value = null;
        if (file.exists()) {
            value = read(file, clz);
            return value == null ? Optional.<T>absent() : Optional.of(value);
        } else if (create) {
            return store(create());
        } else {
            return Optional.absent();
        }
    }

    public Optional<T> store(T newValue) {
        if (newValue == null) {
            return Optional.absent();
        }
        if (write(file, newValue)) {
            value = newValue;
            return Optional.of(value);
        } else {
            return Optional.absent();
        }
    }

    protected abstract T create();

    @SuppressWarnings("unchecked")
    private T read(File file, Class clz) {
        try {
            byte[] bs = java.nio.file.Files.readAllBytes(file.toPath());
            return (T) serializer.deSerialize(bs, clz);
        } catch (Exception e) {
            LOGGER.error("read local file exception: {}", file.getAbsolutePath(), e);
            return null;
        }
    }

    private boolean write(File file, T value) {
        try {
            com.google.common.io.Files.write(serializer.serializeToBytes(value), file);
            return true;
        } catch (Exception e) {
            LOGGER.error("write local file exception: {}", file.getAbsolutePath(), e);
            return false;
        }
    }
}
