package com.snowflake.kafka.connector.format.avro;

import com.snowflake.kafka.connector.WonderSnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.storage.AzureBlobStorage;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;

/**
 * @author stephen
 */

public class AvroFormat implements Format<WonderSnowflakeSinkConnectorConfig, String> {
    private final AzureBlobStorage storage;
    private final AvroData avroData;

    public AvroFormat(AzureBlobStorage storage) {
        this.storage = storage;
        this.avroData = new AvroData(storage.conf().avroDataConfig());
    }

    @Override
    public RecordWriterProvider<WonderSnowflakeSinkConnectorConfig> getRecordWriterProvider() {
        return new AvroRecordWriterProvider(storage, avroData);
    }

    @Override
    public SchemaFileReader<WonderSnowflakeSinkConnectorConfig, String> getSchemaFileReader() {
        throw new UnsupportedOperationException("Reading schemas from S3 is not currently supported");
    }

    @Override
    @Deprecated
    public Object getHiveFactory() {
        throw new UnsupportedOperationException(
                "Hive integration is not currently supported in S3 Connector"
        );
    }

    public AvroData getAvroData() {
        return avroData;
    }
}
