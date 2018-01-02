package qunar.tc.qmq.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class OffsetManager {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetManager.class);

    private static final Joiner KEY_JOINER = Joiner.on('@');
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String MAX_ACKED_PULL_LOG_SEQUENCES_FILE = "max_acked_pull_log_sequences.json";
    private static final String MAX_PULLED_MESSAGE_SEQUENCES_FILE = "max_pulled_message_sequences.json";

    private final ConcurrentMap<String, MaxAckedPullLogSequence> maxAckedPullLogSequences;
    private final ConcurrentMap<String, MaxPulledMessageSequence> maxPulledMessageSequences;

    private final CheckpointStore<ConcurrentMap<String, MaxAckedPullLogSequence>> maxAckedPullLogSequencesStore;
    private final CheckpointStore<ConcurrentMap<String, MaxPulledMessageSequence>> maxPulledMessageSequencesStore;

    private final PeriodicFlushService flushService;

    public OffsetManager(final MessageStoreConfig config) {
        this.maxAckedPullLogSequencesStore = new CheckpointStore<>(config.getCheckpointStorePath(), MAX_ACKED_PULL_LOG_SEQUENCES_FILE, new MaxAckedPullLogSequenceSerde());
        this.maxPulledMessageSequencesStore = new CheckpointStore<>(config.getCheckpointStorePath(), MAX_PULLED_MESSAGE_SEQUENCES_FILE, new MaxPulledMessageSequenceSerde());

        this.maxAckedPullLogSequences = load(maxAckedPullLogSequencesStore);
        this.maxPulledMessageSequences = load(maxPulledMessageSequencesStore);

        this.flushService = new PeriodicFlushService(new SequencesFlushProvider());

        flushService.start();
    }

    private static String buildKey(final String... parts) {
        return KEY_JOINER.join(parts);
    }

    private <T> ConcurrentMap<String, T> load(final CheckpointStore<ConcurrentMap<String, T>> store) {
        final ConcurrentMap<String, T> sequences = store.loadCheckpoint();
        if (sequences == null) {
            return new ConcurrentHashMap<>();
        } else {
            return sequences;
        }
    }

    public List<MaxAckedPullLogSequence> getMaxAckedPullLogSequences() {
        return new ArrayList<>(maxAckedPullLogSequences.values());
    }

    public List<MaxPulledMessageSequence> getMaxPulledMessageSequences() {
        return new ArrayList<>(maxPulledMessageSequences.values());
    }

    public MaxAckedPullLogSequence getOrCreateMaxAckedPullLogSequence(final String subject, final String group, final String consumerId) {
        final String key = buildKey(subject, group, consumerId);
        if (!maxAckedPullLogSequences.containsKey(key)) {
            maxAckedPullLogSequences.putIfAbsent(key, new MaxAckedPullLogSequence(subject, group, consumerId, -1));
        }
        return maxAckedPullLogSequences.get(key);
    }

    public MaxPulledMessageSequence getOrCreateMaxPulledMessageSequence(final String subject, final String group) {
        final String key = buildKey(subject, group);
        if (!maxPulledMessageSequences.containsKey(key)) {
            maxPulledMessageSequences.putIfAbsent(key, new MaxPulledMessageSequence(subject, group, -1));
        }
        return maxPulledMessageSequences.get(key);
    }

    public void shutdown() {
        flushService.shutdown();
    }

    private static final class MaxAckedPullLogSequenceSerde implements CheckpointStore.Serde<ConcurrentMap<String, MaxAckedPullLogSequence>> {
        @Override
        public byte[] toBytes(ConcurrentMap<String, MaxAckedPullLogSequence> value) {
            try {
                return MAPPER.writeValueAsBytes(value.values());
            } catch (JsonProcessingException e) {
                throw new RuntimeException("serialize consume offsets failed.", e);
            }
        }

        @Override
        public ConcurrentMap<String, MaxAckedPullLogSequence> fromBytes(byte[] data) {
            try {
                final List<MaxAckedPullLogSequence> sequences = MAPPER.readValue(data, new TypeReference<List<MaxAckedPullLogSequence>>() {
                });
                final ConcurrentMap<String, MaxAckedPullLogSequence> result = new ConcurrentHashMap<>();
                for (final MaxAckedPullLogSequence sequence : sequences) {
                    final String key = buildKey(sequence.getSubject(), sequence.getGroup(), sequence.getConsumerId());
                    result.put(key, sequence);
                }
                return result;
            } catch (IOException e) {
                throw new RuntimeException("deserialize consume offsets failed.", e);
            }
        }
    }

    private static final class MaxPulledMessageSequenceSerde implements CheckpointStore.Serde<ConcurrentMap<String, MaxPulledMessageSequence>> {
        @Override
        public byte[] toBytes(ConcurrentMap<String, MaxPulledMessageSequence> value) {
            try {
                return MAPPER.writeValueAsBytes(value.values());
            } catch (JsonProcessingException e) {
                throw new RuntimeException("serialize consume offsets failed.", e);
            }
        }

        @Override
        public ConcurrentMap<String, MaxPulledMessageSequence> fromBytes(byte[] data) {
            try {
                final List<MaxPulledMessageSequence> sequences = MAPPER.readValue(data, new TypeReference<List<MaxPulledMessageSequence>>() {
                });

                final ConcurrentMap<String, MaxPulledMessageSequence> result = new ConcurrentHashMap<>();
                for (final MaxPulledMessageSequence sequence : sequences) {
                    final String key = buildKey(sequence.getSubject(), sequence.getGroup());
                    result.put(key, sequence);
                }
                return result;
            } catch (IOException e) {
                throw new RuntimeException("deserialize consume offsets failed.", e);
            }
        }
    }

    private class SequencesFlushProvider implements PeriodicFlushService.FlushProvider {
        @Override
        public int getJoinTime() {
            return 2000;
        }

        @Override
        public int getInterval() {
            return 500;
        }

        @Override
        public void flush() {
            maxAckedPullLogSequencesStore.saveCheckpoint(maxAckedPullLogSequences);
            maxPulledMessageSequencesStore.saveCheckpoint(maxPulledMessageSequences);
        }
    }
}
