package com.snowflake.kafka.connector.format.bytearray;

import com.azure.storage.blob.specialized.BlobOutputStream;
import com.snowflake.kafka.connector.WonderSnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.storage.AzureBlobStorage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author stephen
 */

public class ByteArrayRecordWriterProvider implements RecordWriterProvider<WonderSnowflakeSinkConnectorConfig> {

    private static final Logger log = LoggerFactory.getLogger(ByteArrayRecordWriterProvider.class);
    private final AzureBlobStorage storage;
    private final ByteArrayConverter converter;
    private final String extension;
    private final byte[] lineSeparatorBytes;

    ByteArrayRecordWriterProvider(AzureBlobStorage storage, ByteArrayConverter converter) {
        this.storage = storage;
        this.converter = converter;
        this.extension = storage.conf().getByteArrayExtension();
        this.lineSeparatorBytes = storage.conf().getFormatByteArrayLineSeparator().getBytes();
    }

    @Override
    public String getExtension() {
        return extension;
    }

    @Override
    public RecordWriter getRecordWriter(final WonderSnowflakeSinkConnectorConfig conf, final String filename) {
        return new RecordWriter() {
            final BlobOutputStream outputStream = storage.create(filename, true);

            @Override
            public void write(SinkRecord record) {
                log.trace("Sink record: {}", record);
                try {
                    byte[] bytes = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
                    outputStream.write(bytes);
                    outputStream.write(lineSeparatorBytes);
                } catch (DataException e) {
                    throw new ConnectException(e);
                }
            }

            @Override
            public void commit() {
                outputStream.flush();
            }

            @Override
            public void close() {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }
        };
    }
}
