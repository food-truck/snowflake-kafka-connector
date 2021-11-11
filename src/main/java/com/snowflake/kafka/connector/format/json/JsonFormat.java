package com.snowflake.kafka.connector.format.json;

import com.snowflake.kafka.connector.WonderSnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.storage.AzureBlobStorage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import org.apache.kafka.connect.json.JsonConverter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author stephen
 */

public class JsonFormat implements Format<WonderSnowflakeSinkConnectorConfig, String> {
    private final AzureBlobStorage storage;
    private final JsonConverter converter;

    public JsonFormat(AzureBlobStorage storage) {
        this.storage = storage;
        this.converter = new JsonConverter();
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", "false");
        converterConfig.put("schemas.cache.size", String.valueOf(storage.conf().get(WonderSnowflakeSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG)));
        this.converter.configure(converterConfig, false);
    }

    @Override
    public RecordWriterProvider<WonderSnowflakeSinkConnectorConfig> getRecordWriterProvider() {
        return new JsonRecordWriterProvider(storage, converter);
    }

    @Override
    public SchemaFileReader<WonderSnowflakeSinkConnectorConfig, String> getSchemaFileReader() {
        throw new UnsupportedOperationException("Reading schemas from blob is not currently supported");
    }

    @Override
    @Deprecated
    public Object getHiveFactory() {
        throw new UnsupportedOperationException(
                "Hive integration is not currently supported in S3 Connector"
        );
    }
}
