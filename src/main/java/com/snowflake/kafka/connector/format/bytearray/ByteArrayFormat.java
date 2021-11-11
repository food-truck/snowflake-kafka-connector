package com.snowflake.kafka.connector.format.bytearray;

import com.snowflake.kafka.connector.WonderSnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.storage.AzureBlobStorage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import org.apache.kafka.connect.converters.ByteArrayConverter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author stephen
 */

public class ByteArrayFormat implements Format<WonderSnowflakeSinkConnectorConfig, String> {
  private final AzureBlobStorage storage;
  private final ByteArrayConverter converter;

  public ByteArrayFormat(AzureBlobStorage storage) {
    this.storage = storage;
    this.converter = new ByteArrayConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    this.converter.configure(converterConfig, false);
  }

  @Override
  public RecordWriterProvider<WonderSnowflakeSinkConnectorConfig> getRecordWriterProvider() {
    return new ByteArrayRecordWriterProvider(storage, converter);
  }

  @Override
  public SchemaFileReader<WonderSnowflakeSinkConnectorConfig, String> getSchemaFileReader() {
    throw new UnsupportedOperationException("Reading schemas from Azure blob is not currently supported");
  }

  @Override
  @Deprecated
  public Object getHiveFactory() {
    throw new UnsupportedOperationException(
            "Hive integration is not currently supported in S3 Connector"
    );
  }
}
