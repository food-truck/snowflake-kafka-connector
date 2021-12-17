package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.storage.AzureBlobStorage;
import io.confluent.connect.storage.errors.PartitionException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.common.util.StringUtils;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import io.confluent.connect.storage.partitioner.TimestampExtractor;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;
import io.confluent.connect.storage.util.DateTimeUtils;

/**
 * @author stephen
 */

public class TopicPartitionWriter {
    private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);

    private static final Time SYSTEM_TIME = new SystemTime();
    private final Map<String, String> commitFiles;
    private final Map<String, RecordWriter> writers;
    private final Map<String, Schema> currentSchemas;
    private final TopicPartition tp;
    private final AzureBlobStorage storage;
    private final Partitioner<?> partitioner;
    private final TimestampExtractor timestampExtractor;
    private final Queue<SinkRecord> buffer;
    private final SinkTaskContext context;
    private final int flushSize;
    private final long rotateIntervalMs;
    private final long rotateScheduleIntervalMs;
    private final RecordWriterProvider<WonderSnowflakeSinkConnectorConfig> writerProvider;
    private long nextScheduledRotation;
    private long currentOffset;
    private Long currentStartOffset;
    private Long currentTimestamp;
    private String currentEncodedPartition;
    private Long baseRecordTimestamp;
    private Long offsetToCommit;
    private final Map<String, Long> startOffsets;
    private final Map<String, Long> endOffsets;
    private final Map<String, Long> recordCounts;
    private final long timeoutMs;
    private long failureTime;
    private final StorageSchemaCompatibility compatibility;
    private final String extension;
    private final String zeroPadOffsetFormat;
    private final String dirDelimiter;
    private final String fileDelimiter;
    private final Time time;
    private DateTimeZone timeZone;
    private final WonderSnowflakeSinkConnectorConfig connectorConfig;
    private final String topicsDir;
    private State state;
    private int recordCount;
    private final ErrantRecordReporter reporter;
    private final SnowflakeConnectionService conn;
    private final String tableName;
    private final String sasToken;
    private String currentOp;
    private String currentCmdInsertOrMerge;
    private String pkName;

    public TopicPartitionWriter(TopicPartition tp,
                                AzureBlobStorage storage,
                                SnowflakeConnectionService conn,
                                String sasToken,
                                RecordWriterProvider<WonderSnowflakeSinkConnectorConfig> writerProvider,
                                Partitioner<?> partitioner,
                                WonderSnowflakeSinkConnectorConfig connectorConfig,
                                SinkTaskContext context,
                                ErrantRecordReporter reporter) {
        this(tp, storage, conn, sasToken, writerProvider, partitioner, connectorConfig, context, SYSTEM_TIME, reporter);
    }

    // Visible for testing
    TopicPartitionWriter(TopicPartition tp,
                         AzureBlobStorage storage,
                         SnowflakeConnectionService conn,
                         String sasToken,
                         RecordWriterProvider<WonderSnowflakeSinkConnectorConfig> writerProvider,
                         Partitioner<?> partitioner,
                         WonderSnowflakeSinkConnectorConfig connectorConfig,
                         SinkTaskContext context,
                         Time time,
                         ErrantRecordReporter reporter) {
        this.connectorConfig = connectorConfig;
        this.time = time;
        this.tp = tp;
        this.storage = storage;
        this.conn = conn;
        this.sasToken = sasToken;
        this.tableName = tp.topic().substring(tp.topic().lastIndexOf('.') + 1);
        this.context = context;
        this.writerProvider = writerProvider;
        this.partitioner = partitioner;
        this.reporter = reporter;
        this.timestampExtractor = partitioner instanceof TimeBasedPartitioner
                ? ((TimeBasedPartitioner) partitioner).getTimestampExtractor()
                : null;
        flushSize = connectorConfig.getInt(WonderSnowflakeSinkConnectorConfig.FLUSH_SIZE_CONFIG);
        topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
        rotateIntervalMs = connectorConfig.getLong(WonderSnowflakeSinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG);
        if (rotateIntervalMs > 0 && timestampExtractor == null) {
            log.warn("Property '{}' is set to '{}ms' but partitioner is not an instance of '{}'. This property"
                            + " is ignored.",
                    WonderSnowflakeSinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
                    rotateIntervalMs,
                    TimeBasedPartitioner.class.getName()
            );
        }
        rotateScheduleIntervalMs = connectorConfig.getLong(WonderSnowflakeSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG);
        if (rotateScheduleIntervalMs > 0) {
            timeZone = DateTimeZone.forID(connectorConfig.getString(PartitionerConfig.TIMEZONE_CONFIG));
        }
        timeoutMs = connectorConfig.getLong(WonderSnowflakeSinkConnectorConfig.RETRY_BACKOFF_CONFIG);
        compatibility = StorageSchemaCompatibility.getCompatibility(connectorConfig.getString(StorageSinkConnectorConfig.SCHEMA_COMPATIBILITY_CONFIG));

        buffer = new LinkedList<>();
        commitFiles = new HashMap<>();
        writers = new HashMap<>();
        currentSchemas = new HashMap<>();
        startOffsets = new HashMap<>();
        endOffsets = new HashMap<>();
        recordCounts = new HashMap<>();
        state = State.WRITE_STARTED;
        failureTime = -1L;
        currentOffset = -1L;
        dirDelimiter = connectorConfig.getString(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
        fileDelimiter = connectorConfig.getString(StorageCommonConfig.FILE_DELIM_CONFIG);
        extension = writerProvider.getExtension();
        zeroPadOffsetFormat = "%0"
                + connectorConfig.getInt(WonderSnowflakeSinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG)
                + "d";

        // Initialize scheduled rotation timer if applicable
        setNextScheduledRotation();
    }

    private enum State {
        WRITE_STARTED,
        WRITE_PARTITION_PAUSED,
        SHOULD_ROTATE,
        FILE_COMMITTED;

        private static final State[] VALUES = values();

        public State next() {
            return VALUES[(ordinal() + 1) % VALUES.length];
        }
    }

    public void write() {
        long now = time.milliseconds();
        if (failureTime > 0 && now - failureTime < timeoutMs) {
            return;
        } else {
            failureTime = -1;
        }

        while (!buffer.isEmpty()) {
            try {
                executeState(now);
            } catch (IllegalWorkerStateException e) {
                throw new ConnectException(e);
            } catch (SchemaProjectorException e) {
                if (reporter != null) {
                    reporter.report(buffer.poll(), e);
                    log.warn("Errant record written to DLQ due to: {}", e.getMessage());
                } else {
                    throw e;
                }
            }
        }
        commitOnTimeIfNoData(now);
    }

    @SuppressWarnings("fallthrough")
    private void executeState(long now) {
        switch (state) {
            case WRITE_STARTED:
                pause();
                nextState();
                // fallthrough
            case WRITE_PARTITION_PAUSED:
                SinkRecord record = buffer.peek();
                if (timestampExtractor != null) {
                    currentTimestamp = timestampExtractor.extract(record, now);
                    if (baseRecordTimestamp == null) {
                        baseRecordTimestamp = currentTimestamp;
                    }
                }

                assert record != null;
                Schema valueSchema = record.valueSchema();
                String encodedPartition;
                try {
                    encodedPartition = partitioner.encodePartition(record, now);
                } catch (PartitionException e) {
                    if (reporter != null) {
                        reporter.report(record, e);
                        buffer.poll();
                        break;
                    } else {
                        throw e;
                    }
                }

                Schema currentValueSchema = currentSchemas.get(encodedPartition);
                if (currentValueSchema == null) {
                    currentSchemas.put(encodedPartition, valueSchema);
                    currentValueSchema = valueSchema;
                }

                if (currentCmdInsertOrMerge == null) {
                    //init currentOp the first time
                    currentOp = getOp(record);
                    if (currentOp.equals("r")) {
                        currentCmdInsertOrMerge = "insert";
                    }
                }

                if (pkName == null) {
                    pkName = getPkName(record);
                }

                if (!checkRotationOrAppend(record, currentValueSchema, valueSchema, encodedPartition, now)) {
                    break;
                }
                // fallthrough
            case SHOULD_ROTATE:
                commitFiles();
                nextState();
                // fallthrough
            case FILE_COMMITTED:
                setState(State.WRITE_PARTITION_PAUSED);
                break;
            default:
                log.error("{} is not a valid state to write record for topic partition {}.", state, tp);
        }
    }

    /**
     * Check if we should rotate the file (schema change, time-based).
     *
     * @returns true if rotation is being performed, false otherwise
     */
    private boolean checkRotationOrAppend(SinkRecord record, Schema currentValueSchema, Schema valueSchema, String encodedPartition, long now) {
        // rotateOnTime is safe to go before writeRecord, because it is acceptable
        // even for a faulty record to trigger time-based rotation if it applies
        // comment because this will generate 1 record size commit
//        if (rotateOnTime(encodedPartition, currentTimestamp, now)) {
//            setNextScheduledRotation();
//            nextState();
//            return true;
//        }

        if (compatibility.shouldChangeSchema(record, null, currentValueSchema) && recordCount > 0) {
            // This branch is never true for the first record read by this TopicPartitionWriter
            log.trace("Incompatible change of schema detected for record '{}' with encoded partition "
                            + "'{}' and current offset: '{}'",
                    record,
                    encodedPartition,
                    currentOffset);
            currentSchemas.put(encodedPartition, valueSchema);
            nextState();
            return true;
        }

        SinkRecord projectedRecord = compatibility.project(record, null, currentValueSchema);
        boolean validRecord = writeRecord(projectedRecord, encodedPartition);
        buffer.poll();
        if (!validRecord) {
            // skip the faulty record and don't rotate
            return false;
        }

        if (rotateOnSize()) {
            log.info("Starting commit and rotation for topic partition {} with start offset {}", tp, startOffsets);
            nextState();
            return true;
        }

        if (rotateOnMode(record)) {
            log.info("Starting commit and rotation on mode for topic partition {} with start offset {}", tp, startOffsets);
            nextState();
            return true;
        }

        return false;
    }

    private String getOp(SinkRecord record) {
        Struct recordValue = Requirements.requireStruct(record.value(), "Read record to set topic routing for CREATE / UPDATE");
        return recordValue.getString(WonderSnowflakeSinkConnectorConfig.OPERATION_FIELD);
    }

    private String getPkName(SinkRecord record) {
        return record.keySchema().fields().get(0).name();
    }

    private boolean rotateOnMode(SinkRecord record) {
        String op = getOp(record);
        // r -> others or others -> r means need rotate
        if ((currentOp.equals(WonderSnowflakeSinkConnectorConfig.OPERATION_SNAPSHOT) && !op.equals(WonderSnowflakeSinkConnectorConfig.OPERATION_SNAPSHOT))
                || (!currentOp.equals(WonderSnowflakeSinkConnectorConfig.OPERATION_SNAPSHOT) && op.equals(WonderSnowflakeSinkConnectorConfig.OPERATION_SNAPSHOT))) {
            if (currentOp.equals(WonderSnowflakeSinkConnectorConfig.OPERATION_SNAPSHOT)) {
                currentCmdInsertOrMerge = "insert";
            } else {
                currentCmdInsertOrMerge = "merge";
            }
            currentOp = op;
            return true;
        }
        return false;
    }

    private void commitOnTimeIfNoData(long now) {
        if (buffer.isEmpty()) {
            // committing files after waiting for rotateIntervalMs time but less than flush.size
            // records available
            if (recordCount > 0 && rotateOnTime(currentEncodedPartition, currentTimestamp, now)) {
                log.info("Committing files after waiting for rotateIntervalMs time but less than flush.size records available.");
                setNextScheduledRotation();
                commitFiles();
            }

            resume();
            setState(State.WRITE_STARTED);
        }
    }

    public void close() throws ConnectException {
        log.debug("Closing TopicPartitionWriter {}", tp);
        for (RecordWriter writer : writers.values()) {
            writer.close();
        }
        writers.clear();
        startOffsets.clear();
    }

    public void buffer(SinkRecord sinkRecord) {
        buffer.add(sinkRecord);
    }

    public Long getOffsetToCommitAndReset() {
        Long latest = offsetToCommit;
        offsetToCommit = null;
        return latest;
    }

    public Long currentStartOffset() {
        return currentStartOffset;
    }

    public void failureTime(long when) {
        this.failureTime = when;
    }

    private Long minStartOffset() {
        Optional<Long> minStartOffset = startOffsets.values().stream().min(Comparator.comparing(Long::valueOf));
        return minStartOffset.orElse(null);
    }

    private String getDirectoryPrefix(String encodedPartition) {
        return partitioner.generatePartitionedPath(tp.topic(), encodedPartition);
    }

    private void nextState() {
        state = state.next();
    }

    private void setState(State state) {
        this.state = state;
    }

    private boolean rotateOnTime(String encodedPartition, Long recordTimestamp, long now) {
        if (recordCount <= 0) {
            return false;
        }
        // rotateIntervalMs > 0 implies timestampExtractor != null
        boolean periodicRotation = rotateIntervalMs > 0
                && timestampExtractor != null
                && (recordTimestamp - baseRecordTimestamp >= rotateIntervalMs
                        || !encodedPartition.equals(currentEncodedPartition));

        log.trace("Checking rotation on time with recordCount '{}' and encodedPartition '{}'", recordCount, encodedPartition);

        log.trace("Should apply periodic time-based rotation (rotateIntervalMs: '{}', baseRecordTimestamp: "
                        + "'{}', timestamp: '{}', encodedPartition: '{}', currentEncodedPartition: '{}')? {}",
                rotateIntervalMs,
                baseRecordTimestamp,
                recordTimestamp,
                encodedPartition,
                currentEncodedPartition,
                periodicRotation
        );

        boolean scheduledRotation = rotateScheduleIntervalMs > 0 && now >= nextScheduledRotation;
        log.trace("Should apply scheduled rotation: (rotateScheduleIntervalMs: '{}', nextScheduledRotation: '{}', now: '{}')? {}",
                rotateScheduleIntervalMs,
                nextScheduledRotation,
                now,
                scheduledRotation
        );
        return periodicRotation || scheduledRotation;
    }

    private void setNextScheduledRotation() {
        if (rotateScheduleIntervalMs > 0) {
            long now = time.milliseconds();
            nextScheduledRotation = DateTimeUtils.getNextTimeAdjustedByDay(now, rotateScheduleIntervalMs, timeZone);
            if (log.isDebugEnabled()) {
                log.debug("Update scheduled rotation timer. Next rotation for {} will be at {}", tp, new DateTime(nextScheduledRotation).withZone(timeZone));
            }
        }
    }

    private boolean rotateOnSize() {
        boolean messageSizeRotation = recordCount >= flushSize;
        log.trace("Should apply size-based rotation (count {} >= flush size {})? {}", recordCount, flushSize, messageSizeRotation);
        return messageSizeRotation;
    }

    private void pause() {
        log.trace("Pausing writer for topic-partition '{}'", tp);
        context.pause(tp);
    }

    private void resume() {
        log.trace("Resuming writer for topic-partition '{}'", tp);
        context.resume(tp);
    }

    private RecordWriter newWriter(SinkRecord record, String encodedPartition) throws ConnectException {
        String commitFilename = getCommitFilename(encodedPartition);
        log.debug("Creating new writer encodedPartition='{}' filename='{}'", encodedPartition, commitFilename);
        RecordWriter writer = writerProvider.getRecordWriter(connectorConfig, commitFilename);
        writers.put(encodedPartition, writer);
        return writer;
    }

    private String getCommitFilename(String encodedPartition) {
        String commitFile;
        if (commitFiles.containsKey(encodedPartition)) {
            commitFile = commitFiles.get(encodedPartition);
        } else {
            long startOffset = startOffsets.get(encodedPartition);
            String prefix = getDirectoryPrefix(encodedPartition);
            commitFile = fileKeyToCommit(prefix, startOffset);
            commitFiles.put(encodedPartition, commitFile);
        }
        return commitFile;
    }

    private String fileKey(String topicsPrefix, String keyPrefix, String name) {
        String suffix = keyPrefix + dirDelimiter + name;
        return StringUtils.isNotBlank(topicsPrefix) ? topicsPrefix + dirDelimiter + suffix : suffix;
    }

    private String fileKeyToCommit(String dirPrefix, long startOffset) {
        String name = tp.topic()
                + fileDelimiter
                + tp.partition()
                + fileDelimiter
                + String.format(zeroPadOffsetFormat, startOffset)
                + extension;
        return fileKey(topicsDir, dirPrefix, name);
    }

    private boolean writeRecord(SinkRecord record, String encodedPartition) {
        RecordWriter writer = writers.get(encodedPartition);
        long currentOffsetIfSuccessful = record.kafkaOffset();
        boolean shouldRemoveWriter = false;
        boolean shouldRemoveStartOffset = false;
        boolean shouldRemoveCommitFilename = false;
        try {
            if (!startOffsets.containsKey(encodedPartition)) {
                log.trace("Setting writer's start offset for '{}' to {}", encodedPartition, currentOffsetIfSuccessful);
                startOffsets.put(encodedPartition, currentOffsetIfSuccessful);
                shouldRemoveStartOffset = true;
            }
            if (writer == null) {
                if (!commitFiles.containsKey(encodedPartition)) {
                    shouldRemoveCommitFilename = true;
                }
                writer = newWriter(record, encodedPartition);
                shouldRemoveWriter = true;
            }
            writer.write(record);
        } catch (DataException e) {
            if (reporter != null) {
                if (shouldRemoveStartOffset) {
                    startOffsets.remove(encodedPartition);
                }
                if (shouldRemoveWriter) {
                    writers.remove(encodedPartition);
                }
                if (shouldRemoveCommitFilename) {
                    commitFiles.remove(encodedPartition);
                }
                reporter.report(record, e);
                log.warn("Errant record written to DLQ due to: {}", e.getMessage());
                return false;
            } else {
                throw new ConnectException(e);
            }
        }

        currentEncodedPartition = encodedPartition;
        currentOffset = record.kafkaOffset();
        if (shouldRemoveStartOffset) {
            log.trace("Setting writer's start offset for '{}' to {}", currentEncodedPartition, currentOffset);

            // Once we have a "start offset" for a particular "encoded partition"
            // value, we know that we have at least one record. This allows us
            // to initialize all our maps at the same time, and saves future
            // checks on the existence of keys
            recordCounts.put(currentEncodedPartition, 0L);
            endOffsets.put(currentEncodedPartition, 0L);
        }
        ++recordCount;

        recordCounts.put(currentEncodedPartition, recordCounts.get(currentEncodedPartition) + 1);
        endOffsets.put(currentEncodedPartition, currentOffset);
        return true;
    }

    private void commitFiles() {
        currentStartOffset = minStartOffset();
        for (Map.Entry<String, String> entry : commitFiles.entrySet()) {
            String encodedPartition = entry.getKey();
            commitFile(encodedPartition);
            ingestToSnowflake(entry.getValue());
            startOffsets.remove(encodedPartition);
            endOffsets.remove(encodedPartition);
            recordCounts.remove(encodedPartition);
            log.info("Committed {} for {}", entry.getValue(), tp);
        }

        offsetToCommit = currentOffset + 1;
        commitFiles.clear();
        currentSchemas.clear();
        recordCount = 0;
        baseRecordTimestamp = null;
        log.info("Files committed to Azure. Target commit offset for {} is {}", tp, offsetToCommit);
    }

    private void commitFile(String encodedPartition) {
        if (!startOffsets.containsKey(encodedPartition)) {
            log.warn("Tried to commit file with missing starting offset partition: {}. Ignoring.", encodedPartition);
            return;
        }

        if (writers.containsKey(encodedPartition)) {
            RecordWriter writer = writers.get(encodedPartition);
            // Commits the file and closes the underlying output stream.
            writer.commit();
            writers.remove(encodedPartition);
            log.debug("Removed writer for '{}'", encodedPartition);
        }
    }

    private void ingestToSnowflake(String path) {
        createDstTableIfNotExists(tableName);
        String tmpTable = createTmpTable();
        copyIntoTmp(tmpTable, path);
        if (currentCmdInsertOrMerge.equals("insert")) {
            insertIntoDstFromTmp(tmpTable, tableName);
        } else {
            mergeIntoDstFromTmp(tmpTable, tableName, pkName);
        }
        dropTmpTable(tmpTable);
    }

    private void createDstTableIfNotExists(String tableName) {
        if (tableNotExistOrSchemaChange(tableName)) {
            String query = String.format("CREATE TABLE %s (%s) IF NOT EXISTS", tableName, columnsForDefine() + ",__SF_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()");
            execute(query);
        }
    }

    private boolean tableNotExistOrSchemaChange(String tableName) {
        String query = String.format("DESC TABLE %s", tableName);
        PreparedStatement stmt;
        try {
            stmt = conn.getConnection().prepareStatement(query);
            ResultSet resultSet = stmt.executeQuery();
            List<String> existColumns = new ArrayList<>();
            while (resultSet.next()) {
                existColumns.add(resultSet.getString("name"));
            }
            List<String> missColumns = getMissedColumns(existColumns);
            stmt.close();
            if (missColumns.size() != 0) {
                alterTableAddMissColumns(tableName, missColumns);
            }
            return false;
        } catch (SQLException e) {
            log.info(e.getMessage());
            return true;
        }
    }

    private void alterTableAddMissColumns(String tableName, List<String> missColumns) {
        String query = String.format("ALTER TABLE %s ADD COLUMN %s", tableName, columnsForSetMiss(missColumns));
        execute(query);
    }

    private List<String> getMissedColumns(List<String> existColumns) {
        Set<String> existColumnSet = new HashSet<>(existColumns);
        return currentFields().stream().filter(item -> !existColumnSet.contains(item.name().toUpperCase())).map(item -> item.name().toUpperCase()).collect(Collectors.toList());
    }

    private String createTmpTable() {
        String tmpTableName =  String.format("%s_%d", tableName, currentTimestamp);
        String query = String.format("CREATE TEMPORARY TABLE %s (%s VARIANT)", tmpTableName, WonderSnowflakeSinkConnectorConfig.SNOWFLAKE_TEMPORARY_COLUMN_NAME);
        execute(query);
        return tmpTableName;
    }

    private void dropTmpTable(String name) {
        String query = String.format("DROP TABLE %s", name);
        execute(query);
    }

    private void copyIntoTmp(String name, String path) {
        String query = String.format("COPY INTO %s from 'azure://%s/%s' CREDENTIALS = (AZURE_SAS_TOKEN = '%s') FILE_FORMAT = (TYPE = 'json')",
                name,
                storage.url().substring("http://".length() + 1),
                path,
                sasToken);
        execute(query);
    }

    private void insertIntoDstFromTmp(String src, String dst) {
        String query = String.format("INSERT INTO %s (%s) SELECT %s FROM %s",
                dst,
                columns(),
                columnsFromJson(),
                src);
        execute(query);
    }

    private void mergeIntoDstFromTmp(String src, String dst, String pk) {
        String query = String.format("MERGE INTO %s AS DST USING (SELECT %s FROM %s) AS SRC ON SRC.%S=DST.%S WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
                dst,
                columnsFromJson(),
                src,
                pk,
                pk,
                columnsForSet(),
                columns(),
                columns());
        execute(query);
    }

    private List<Field> currentFields() {
        return currentSchemas.get(currentEncodedPartition).fields();
    }

    private String columns() {
        return currentFields().stream().map(Field::name).collect(Collectors.joining(","));
    }

    private String columnsFromJson() {
        return currentFields().stream().map(item -> String.format("raw:%s::%s as %s", item.name(), sqlTypeMapper(item.schema()), item.name())).collect(Collectors.joining(","));
    }

    private String columnsForSet() {
        return currentFields().stream().map(item -> String.format("DST.%s=src.%s", item.name(), item.name())).collect(Collectors.joining(","));
    }

    private String columnsForDefine() {
        return currentFields().stream().map(item -> String.format("%s %s", item.name(), sqlTypeMapper(item.schema()))).collect(Collectors.joining(","));
    }

    private Object columnsForSetMiss(List<String> missColumns) {
        return currentFields().stream().filter(item -> missColumns.contains(item.name().toUpperCase())).map(item -> String.format("%s %s", item.name(), sqlTypeMapper(item.schema()))).collect(Collectors.joining(","));
    }

    private void execute(String query) {
        log.info(query);
        try {
            PreparedStatement stmt = conn.getConnection().prepareStatement(query);
            stmt.execute();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    private String sqlTypeMapper(Schema field) {
        if (field.name() != null) {
            switch (field.name()) {
                case Decimal.LOGICAL_NAME:
                    return "DECIMAL";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case org.apache.kafka.connect.data.Time.LOGICAL_NAME:
                    return "TIME";
                case "io.debezium.time.ZonedTimestamp":
                case Timestamp.LOGICAL_NAME:
                    return "TIMESTAMP_NTZ";
                default:
                    // pass through to primitive types
            }
        }
        switch (field.type()) {
            case INT8:
                return "TINYINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "REAL";
            case FLOAT64:
                return "DOUBLE PRECISION";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "TEXT";
            case BYTES:
                return "BINARY";
            default:
                return "VARIANT";
        }
    }
}