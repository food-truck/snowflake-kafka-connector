package com.snowflake.kafka.connector.storage;

import com.snowflake.kafka.connector.WonderSnowflakeSinkConnectorConfig;
import junit.framework.TestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author stephen
 */
public class AzureBlobStorageTest extends TestCase {
    private final Logger logger = LoggerFactory.getLogger(AzureBlobStorageTest.class);
    @Test
    public void test() {
        Map<String, String> props = new HashMap<>();
        props.put("azure.account.name", "wonderetldev");
        props.put("azure.account.key", "");
        props.put("azure.container.name", "etl");
        props.put("format.class", "com.snowflake.kafka.connector.format.json.JsonFormat");
        props.put("storage.class", "com.snowflake.kafka.connector.storage.AzureBlobStorage");
        props.put("connector.class", "com.snowflake.kafka.connector.WonderSnowflakeSinkConnector");
        props.put("flush.size", "20000");
        WonderSnowflakeSinkConnectorConfig connectorConfig = new WonderSnowflakeSinkConnectorConfig(props);
        AzureBlobStorage storage = new AzureBlobStorage(connectorConfig, "https://wonderetldev.azure.net");
        logger.info(storage.url());
    }
}