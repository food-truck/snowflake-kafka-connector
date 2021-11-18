package com.snowflake.kafka.connector.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.snowflake.kafka.connector.WonderSnowflakeSinkConnectorConfig;
import io.confluent.connect.storage.Storage;
import io.confluent.connect.storage.common.util.StringUtils;
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.avro.file.SeekableInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

/**
 * @author stephen
 */

public class AzureBlobStorage implements Storage<WonderSnowflakeSinkConnectorConfig, Iterable<ListBlobItem>> {
    private final Logger logger = LoggerFactory.getLogger(AzureBlobStorage.class);

    private final WonderSnowflakeSinkConnectorConfig connectorConfig;
    private final BlobServiceClient blobServiceClient;
    private final BlobContainerClient blobContainerClient;

    public AzureBlobStorage(WonderSnowflakeSinkConnectorConfig connectorConfig, String url) {
        this.connectorConfig = connectorConfig;
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(connectorConfig.accountName(), connectorConfig.accountKey());
        this.blobServiceClient = new BlobServiceClientBuilder().endpoint(connectorConfig.url()).credential(credential).buildClient();
        this.blobContainerClient = blobServiceClient.getBlobContainerClient(connectorConfig.containerName());
        logger.info("Azure Blob Storage URL: {}", url + "/" + connectorConfig.containerName());
    }

    @Override
    public boolean exists(String name) {
        if (!StringUtils.isBlank(name)) {
            return blobContainerClient.getBlobClient(name).exists();
        }
        return false;
    }

    @Override
    public boolean create(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputStream create(String path, WonderSnowflakeSinkConnectorConfig wonderSnowflakeSinkConnectorConfig, boolean overwrite) {
        return create(path, overwrite);
    }

    public BlobOutputStream create(String path, boolean overwrite) {
        if (!overwrite) {
            throw new UnsupportedOperationException("Creating without overwriting is not supported in Azure Connector");
        }
        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("Path can not be empty!");
        }
        return blobContainerClient.getBlobClient(path).getBlockBlobClient().getBlobOutputStream();
    }

    @Override
    public SeekableInput open(String s, WonderSnowflakeSinkConnectorConfig wonderSnowflakeSinkConnectorConfig) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputStream append(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(String s) {

    }

    @Override
    public Iterable<ListBlobItem> list(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {

    }

    @Override
    public String url() {
        return blobContainerClient.getBlobContainerUrl();
    }

    @Override
    public WonderSnowflakeSinkConnectorConfig conf() {
        return connectorConfig;
    }
}
