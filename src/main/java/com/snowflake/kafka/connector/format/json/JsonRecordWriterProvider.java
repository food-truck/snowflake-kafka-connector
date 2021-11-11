package com.snowflake.kafka.connector.format.json;

import com.azure.storage.blob.specialized.BlobOutputStream;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.WonderSnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.storage.AzureBlobStorage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author stephen
 */

public class JsonRecordWriterProvider implements RecordWriterProvider<WonderSnowflakeSinkConnectorConfig> {

    private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
    private static final String EXTENSION = ".json";
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private static final byte[] LINE_SEPARATOR_BYTES = LINE_SEPARATOR.getBytes();
    private final AzureBlobStorage storage;
    private final ObjectMapper mapper;
    private final JsonConverter converter;

    JsonRecordWriterProvider(AzureBlobStorage storage, JsonConverter converter) {
        this.storage = storage;
        this.mapper = new ObjectMapper();
        this.converter = converter;
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter getRecordWriter(final WonderSnowflakeSinkConnectorConfig conf, final String filename) {
        try {
            return new RecordWriter() {
                final BlobOutputStream outputStream = storage.create(filename, true);
                final JsonGenerator writer = mapper.getFactory().createGenerator(outputStream).setRootValueSeparator(null);

                @Override
                public void write(SinkRecord record) {
                    log.trace("Sink record: {}", record);
                    try {
                        Object value = record.value();
                        if (value instanceof Struct) {
                            byte[] rawJson = converter.fromConnectData(record.topic(), record.valueSchema(), value);
                            outputStream.write(rawJson);
                            outputStream.write(LINE_SEPARATOR_BYTES);
                        } else {
                            writer.writeObject(value);
                            writer.writeRaw(LINE_SEPARATOR);
                        }
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }

                @Override
                public void commit() {
                    log.debug("Committing");
                    try {
                        writer.flush();
                        writer.close();
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }

                @Override
                public void close() {
                    log.debug("Closing writer");
                    try {
                        writer.close();
                    } catch (IOException e) {
                        throw new ConnectException(e);
                    }
                }
            };
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }
}
