package com.snowflake.kafka.connector.format.avro;

import com.azure.storage.blob.specialized.BlobOutputStream;
import com.snowflake.kafka.connector.WonderSnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.storage.AzureBlobStorage;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author stephen
 */

import java.io.IOException;

public class AvroRecordWriterProvider implements RecordWriterProvider<WonderSnowflakeSinkConnectorConfig> {
    private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);

    private static final String EXTENSION = ".avro";
    private final AzureBlobStorage storage;
    private final AvroData avroData;

    AvroRecordWriterProvider(AzureBlobStorage storage, AvroData avroData) {
        this.storage = storage;
        this.avroData = avroData;
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter getRecordWriter(final WonderSnowflakeSinkConnectorConfig conf, final String filename) {
        return new RecordWriter() {
            final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
            Schema schema = null;
            BlobOutputStream outputStream;

            @Override
            public void write(SinkRecord record) {
                if (schema == null) {
                    schema = record.valueSchema();
                    try {
                        log.info("Opening record writer for: {}", filename);
                        outputStream = storage.create(filename, true);
                        org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
                        writer.setCodec(CodecFactory.fromString(conf.getAvroCodec()));
                        writer.create(avroSchema, outputStream);
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }
                log.trace("Sink record: {}", record);
                Object value = avroData.fromConnectData(schema, record.value());
                try {
                    if (value instanceof NonRecordContainer) {
                        value = ((NonRecordContainer) value).getValue();
                    }
                    writer.append(value);
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }

            @Override
            public void commit() {
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    throw new RetriableException(e);
                }
            }

            @Override
            public void close() {
                try {
                    writer.close();
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }
        };
    }
}
