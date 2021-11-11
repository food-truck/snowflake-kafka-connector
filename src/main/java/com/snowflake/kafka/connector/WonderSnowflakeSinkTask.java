package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.storage.AzureBlobStorage;
import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author stephen
 */

public class WonderSnowflakeSinkTask extends SinkTask {
    private final Logger logger = LoggerFactory.getLogger(WonderSnowflakeSinkTask.class);

    private WonderSnowflakeSinkConnectorConfig connectorConfig;
    private long timeoutMs;
    private AzureBlobStorage storage;
    private final Map<TopicPartition, TopicPartitionWriter> topicPartitionWriterMap;
    private Partitioner<?> partitioner;
    private Format<WonderSnowflakeSinkConnectorConfig, String> format;
    private RecordWriterProvider<WonderSnowflakeSinkConnectorConfig> writerProvider;
    private final Time time;
    private ErrantRecordReporter reporter;

    public WonderSnowflakeSinkTask() {
        topicPartitionWriterMap = new HashMap<>();
        time = new SystemTime();
    }

    @Override
    public String version() {
        return WonderSnowflakeSinkConnectorConfig.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        long startTime = System.currentTimeMillis();

        connectorConfig = new WonderSnowflakeSinkConnectorConfig(props);
        timeoutMs = connectorConfig.getLong(WonderSnowflakeSinkConnectorConfig.RETRY_BACKOFF_CONFIG);
        logger.info("Starting task[{}], storage[{}]", connectorConfig.name(), connectorConfig.url());

        try {
            storage = StorageFactory.createStorage(AzureBlobStorage.class,
                    WonderSnowflakeSinkConnectorConfig.class,
                    connectorConfig,
                    connectorConfig.url());
            writerProvider = connectorConfig.formatClass().getConstructor(AzureBlobStorage.class).newInstance(storage)
                    .getRecordWriterProvider();
            partitioner = connectorConfig.partitionerClass().getConstructor().newInstance();
            partitioner.configure(connectorConfig.plainValuesWithOriginals());
            reporter = context.errantRecordReporter();
            open(context.assignment());
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new ConnectException("format.class reflection exception: ", e);
        }

        logger.info("Start task[{}] success, time: {} seconds",
                connectorConfig.name(),
                (System.currentTimeMillis() - startTime) / 1000);
    }

    public void open(Collection<TopicPartition> partitions) {
        for (TopicPartition topicPartition : partitions) {
            topicPartitionWriterMap.put(topicPartition, newPartitionWriter(topicPartition));
        }
    }

    private TopicPartitionWriter newPartitionWriter(TopicPartition topicPartition) {
        return new TopicPartitionWriter(
                topicPartition,
                storage,
                writerProvider,
                partitioner,
                connectorConfig,
                context,
                reporter);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.size() == 0) {
            return;
        }

        SinkRecord first = collection.iterator().next();
        logger.info("Received {} records. First record[{}-{}-{}]: {}-{}.",
                collection.size(),
                first.topic(), first.kafkaPartition(), first.kafkaOffset(),
                first.key(), first.value());

        for (SinkRecord record : collection) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());

            if (maybeSkipOnNullValue(record)) {
                continue;
            }

            logger.trace("Record[{}-{}-{}]: {}-{}",
                    first.topic(), first.kafkaPartition(), first.kafkaOffset(),
                    first.key(), first.value());

            topicPartitionWriterMap.get(topicPartition).buffer(record);
        }

        for (TopicPartition topicPartition : topicPartitionWriterMap.keySet()) {
            TopicPartitionWriter topicPartitionWriter = topicPartitionWriterMap.get(topicPartition);
            try {
                topicPartitionWriter.write();
            } catch (RetriableException e) {
                logger.error("Exception on topic partition {}: {}", topicPartition, e);

                Long currentStartOffset = topicPartitionWriter.currentStartOffset();
                if (currentStartOffset != null) {
                    context.offset(topicPartition, currentStartOffset);
                }
                context.timeout(timeoutMs);

                topicPartitionWriter = newPartitionWriter(topicPartition);
                topicPartitionWriter.failureTime(time.milliseconds());

                topicPartitionWriterMap.put(topicPartition, topicPartitionWriter);
            }
        }
    }

    private boolean maybeSkipOnNullValue(SinkRecord record) {
        if (record.value() == null) {
            if (connectorConfig.nullValueBehavior()
                    .equalsIgnoreCase(WonderSnowflakeSinkConnectorConfig.BehaviorOnNullValues.IGNORE.toString())) {
                logger.debug("Null valued record from topic '{}', partition {} and offset {} was skipped.",
                        record.topic(),
                        record.kafkaPartition(),
                        record.kafkaOffset()
                );
                return true;
            } else {
                throw new ConnectException("Null valued records are not writeable with current "
                        + WonderSnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG + " 'settings.");
            }
        }
        return false;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitionWriterMap.keySet()) {
            Long offset = topicPartitionWriterMap.get(topicPartition).getOffsetToCommitAndReset();
            if (offset != null) {
                logger.trace("Forwarding to framework request to commit offset: {} for {}", offset, topicPartition);
                offsetsToCommit.put(topicPartition, new OffsetAndMetadata(offset));
            }
        }
        return offsetsToCommit;
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition topicPartition : topicPartitionWriterMap.keySet()) {
            try {
                topicPartitionWriterMap.get(topicPartition).close();
            } catch (ConnectException e) {
                logger.error("Close writer[{}] failed: {}", topicPartition, e);
            }
        }
        this.topicPartitionWriterMap.clear();
    }

    @Override
    public void stop() {
        logger.info("Stop wonder snowflake sink task[{}]", connectorConfig.name());
        try {
            if (storage != null) {
                storage.close();
            }
        } catch (Exception e) {
            throw new ConnectException(e);
        }
    }
}