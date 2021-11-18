package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.format.avro.AvroFormat;
import com.snowflake.kafka.connector.format.bytearray.ByteArrayFormat;
import com.snowflake.kafka.connector.format.json.JsonFormat;
import com.snowflake.kafka.connector.storage.AzureBlobStorage;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.GenericRecommender;
import io.confluent.connect.storage.common.ParentValueRecommender;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author stephen
 */

public class WonderSnowflakeSinkConnectorConfig extends StorageSinkConnectorConfig {
    public static final String SINK_NAME = "wonder-snowflake-sink-connector";
    public static final String VERSION = "0.0.1";
    private static final String AZURE_COMMON_GROUP = "Azure Common";

    public static final String STORAGE_URL_CONFIG = "azure.url";
    private static final String STORAGE_URL_DOC = "Azure storage url";
    private static final String STORAGE_URL_DISPLAY = "Azure Storage URL";
    private static final String STORAGE_URL_DEFAULT = "blob.core.windows.net";

    public static final String ACCOUNT_NAME_CONFIG = "azure.account.name";
    private static final String ACCOUNT_NAME_DOC = "The account name: Must be between 3-24 alphanumeric characters.";
    private static final String ACCOUNT_NAME_DISPLAY = "Account Name";

    public static final String ACCOUNT_KEY_CONFIG = "azure.account.key";
    private static final String ACCOUNT_KEY_DOC = "The Azure Storage account key.";
    private static final String ACCOUNT_KEY_DISPLAY = "Account Key";

    public static final String CONTAINER_NAME_CONFIG = "azure.container.name";
    private static final String CONTAINER_NAME_DOC = "The container name. Must be between 3-63 alphanumeric and '-' characters";
    private static final String CONTAINER_NAME_DISPLAY = "Container Name";

    public static final String BLOCK_SIZE_CONFIG = "azure.block.size";
    private static final String BLOCK_SIZE_DOC = "The block size of Azure multi-block uploads.";
    private static final String BLOCK_SIZE_DISPLAY = "Azure Block Size";
    private static final int BLOCK_SIZE_DEFAULT = 26214400;

    public static final String COMPRESSION_TYPE_CONFIG = "azure.compression.type";
    private static final String COMPRESSION_TYPE_DOC = "Compression type for file written to Azure. Applied when using JsonFormat or ByteArrayFormat.";
    private static final String COMPRESSION_TYPE_DISPLAY = "Compression Type";
    private static final String COMPRESSION_TYPE_DEFAULT = CompressionType.NONE.name;

    public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
    private static final String BEHAVIOR_ON_NULL_VALUES_DOC = "How to handle records with a null value (i.e. Kafka tombstone records).";
    private static final String BEHAVIOR_ON_NULL_VALUES_DISPLAY = "Behavior for null-valued records";
    private static final String BEHAVIOR_ON_NULL_VALUES_DEFAULT = BehaviorOnNullValues.FAIL.name();

    public static final String FORMAT_BYTEARRAY_EXTENSION_CONFIG = "format.bytearray.extension";
    private static final String FORMAT_BYTEARRAY_EXTENSION_DOC = "Output file extension for when using the ByteArrayFormat.";
    private static final String FORMAT_BYTEARRAY_EXTENSION_DISPLAY = "ByteArrayFormat Output File Extension";
    private static final String FORMAT_BYTEARRAY_EXTENSION_DEFAULT = ".bin";

    public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG = "format.bytearray.separator";
    private static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_DOC = "String inserted between records in the same file for ByteArrayFormat. Defaults to ``System.lineSeparator()`` and may contain escape sequences like ``\\n``. An input record that contains the line separator will look like multiple records in the output Azure object.";
    private static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_DISPLAY = "ByteArrayFormat Line Separator";
    private static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT = null;

    private static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
    private static final String PARTITIONER_CLASS_DOC = "The Partitioner Class";
    private static final String PARTITIONER_CLASS_DISPLAY = "Partitioner Class";

    private final StorageCommonConfig commonConfig;
    private final PartitionerConfig partitionerConfig;

    private final Map<String, ComposableConfig> propertyToConfig = new HashMap<>();
    private final Set<AbstractConfig> allConfigs = new HashSet<>();

    private static final GenericRecommender STORAGE_CLASS_RECOMMENDER = new GenericRecommender();
    private static final GenericRecommender FORMAT_CLASS_RECOMMENDER = new GenericRecommender();
    private static final GenericRecommender PARTITIONER_CLASS_RECOMMENDER = new GenericRecommender();
    private static final ParentValueRecommender AVRO_COMPRESSION_RECOMMENDER = new ParentValueRecommender(FORMAT_CLASS_CONFIG, AvroFormat.class, AVRO_SUPPORTED_CODECS);

    static {
        STORAGE_CLASS_RECOMMENDER.addValidValues(Collections.singletonList(AzureBlobStorage.class));
        FORMAT_CLASS_RECOMMENDER.addValidValues(Arrays.asList(AvroFormat.class, JsonFormat.class, ByteArrayFormat.class));
        PARTITIONER_CLASS_RECOMMENDER.addValidValues(Arrays.asList(DefaultPartitioner.class, HourlyPartitioner.class, DailyPartitioner.class, TimeBasedPartitioner.class, FieldPartitioner.class));
    }

    public enum BehaviorOnNullValues {
        IGNORE, FAIL;
    }

    public WonderSnowflakeSinkConnectorConfig(Map<String, String> props) {
        this(newConfigDef(), props);
    }

    public WonderSnowflakeSinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
        super(configDef, props);
        commonConfig = new StorageCommonConfig(StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER), originalsStrings());
        partitionerConfig = new PartitionerConfig(PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER), originalsStrings());
        addToGlobal(partitionerConfig);
        addToGlobal(commonConfig);
        addToGlobal(this);
    }

    private void addToGlobal(AbstractConfig config) {
        allConfigs.add(config);
        addConfig(config.values(), (ComposableConfig) config);
    }

    private void addConfig(Map<String, ?> parsedProps, ComposableConfig config) {
        for (String key : parsedProps.keySet()) {
            propertyToConfig.put(key, config);
        }
    }

    public Map<String, ?> plainValues() {
        Map<String, Object> map = new HashMap<>();
        for (AbstractConfig config : allConfigs) {
            map.putAll(config.values());
        }
        return map;
    }

    public Map<String, Object> plainValuesWithOriginals() {
        Map<String, Object> plainValues = new HashMap<>(plainValues());
        Map<String, ?> originals = originals();
        for (Map.Entry<String, ?> originalEntrySet : originals.entrySet()) {
            String originalKey = originalEntrySet.getKey();
            if (!plainValues.containsKey(originalKey))
                plainValues.put(originalKey, originals.get(originalKey));
        }
        return plainValues;
    }

    public static ConfigDef newConfigDef() {
        ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
                FORMAT_CLASS_RECOMMENDER,
                AVRO_COMPRESSION_RECOMMENDER
        );

        final String azureCommonGroup = AZURE_COMMON_GROUP;
        int orderInGroup = 0;

        configDef.define(ACCOUNT_KEY_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        ACCOUNT_KEY_DOC,
                        azureCommonGroup,
                        ++orderInGroup,
                        ConfigDef.Width.LONG,
                        ACCOUNT_KEY_DISPLAY)
                .define(ACCOUNT_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        ACCOUNT_NAME_DOC,
                        azureCommonGroup,
                        ++orderInGroup,
                        ConfigDef.Width.LONG,
                        ACCOUNT_NAME_DISPLAY)
                .define(STORAGE_URL_CONFIG,
                        ConfigDef.Type.STRING,
                        STORAGE_URL_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        STORAGE_URL_DOC,
                        azureCommonGroup,
                        ++orderInGroup,
                        ConfigDef.Width.LONG,
                        STORAGE_URL_DISPLAY)
                .define(CONTAINER_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        CONTAINER_NAME_DOC,
                        azureCommonGroup,
                        ++orderInGroup,
                        ConfigDef.Width.LONG,
                        CONTAINER_NAME_DISPLAY)
                .define(BLOCK_SIZE_CONFIG,
                        ConfigDef.Type.INT,
                        BLOCK_SIZE_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        BLOCK_SIZE_DOC,
                        azureCommonGroup,
                        ++orderInGroup,
                        ConfigDef.Width.MEDIUM,
                        BLOCK_SIZE_DISPLAY)
                .define(COMPRESSION_TYPE_CONFIG,
                        ConfigDef.Type.STRING,
                        COMPRESSION_TYPE_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        COMPRESSION_TYPE_DOC,
                        azureCommonGroup,
                        ++orderInGroup,
                        ConfigDef.Width.MEDIUM,
                        COMPRESSION_TYPE_DISPLAY)
                .define(BEHAVIOR_ON_NULL_VALUES_CONFIG,
                        ConfigDef.Type.STRING,
                        BEHAVIOR_ON_NULL_VALUES_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        BEHAVIOR_ON_NULL_VALUES_DOC,
                        azureCommonGroup,
                        ++orderInGroup,
                        ConfigDef.Width.MEDIUM,
                        BEHAVIOR_ON_NULL_VALUES_DISPLAY)
                .define(FORMAT_BYTEARRAY_EXTENSION_CONFIG,
                        ConfigDef.Type.STRING,
                        FORMAT_BYTEARRAY_EXTENSION_DEFAULT,
                        ConfigDef.Importance.LOW,
                        FORMAT_BYTEARRAY_EXTENSION_DOC,
                        azureCommonGroup,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        FORMAT_BYTEARRAY_EXTENSION_DISPLAY)
                .define(FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG,
                        ConfigDef.Type.STRING,
                        FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT,
                        ConfigDef.Importance.LOW,
                        FORMAT_BYTEARRAY_LINE_SEPARATOR_DOC,
                        azureCommonGroup,
                        ++orderInGroup,
                        ConfigDef.Width.MEDIUM,
                        FORMAT_BYTEARRAY_LINE_SEPARATOR_DISPLAY)
                .define(PARTITIONER_CLASS_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.MEDIUM,
                        PARTITIONER_CLASS_DOC,
                        azureCommonGroup,
                        ++orderInGroup,
                        ConfigDef.Width.MEDIUM,
                        PARTITIONER_CLASS_DISPLAY);

        return configDef;
    }

    public String getByteArrayExtension() {
        return getString(FORMAT_BYTEARRAY_EXTENSION_CONFIG);
    }

    public String getFormatByteArrayLineSeparator() {
        // White space is significant for line separators, but ConfigKey trims it out,
        // so we need to check the originals rather than using the normal machinery.
        if (originalsStrings().containsKey(FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG)) {
            return originalsStrings().get(FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG);
        }
        return FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT;
    }

    public String nullValueBehavior() {
        return getString(BEHAVIOR_ON_NULL_VALUES_CONFIG);
    }

    public String accountKey() {
        return getString(ACCOUNT_KEY_CONFIG);
    }

    public String accountName() {
        return getString(ACCOUNT_NAME_CONFIG);
    }

    public int blockSize() {
        return getInt(BLOCK_SIZE_CONFIG);
    }

    public String containerName() {
        return getString(CONTAINER_NAME_CONFIG);
    }

    public String url() {
        return "https://" + accountName() + "." + getString(STORAGE_URL_CONFIG);
    }

    @SuppressWarnings("unchecked")
    public Class<Format<WonderSnowflakeSinkConnectorConfig, String>> formatClass() {
        return (Class<Format<WonderSnowflakeSinkConnectorConfig, String>>) getClass(FORMAT_CLASS_CONFIG);
    }

    @SuppressWarnings("unchecked")
    public Class<? extends Partitioner<?>> partitionerClass() {
        return (Class<? extends Partitioner<?>>) getClass(PARTITIONER_CLASS_CONFIG);
    }

    public String name() {
        return SINK_NAME;
    }
}