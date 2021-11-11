package com.snowflake.kafka.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author stephen
 */

public class WonderSnowflakeSinkConnector extends SinkConnector {
    private final Logger logger = LoggerFactory.getLogger(WonderSnowflakeSinkConnector.class);

    private Map<String, String> configProps;
    private WonderSnowflakeSinkConnectorConfig connectorConfig;

    public WonderSnowflakeSinkConnector() {

    }

    @Override
    public void start(Map<String, String> props) {
        configProps = new HashMap<>(props);
        connectorConfig = new WonderSnowflakeSinkConnectorConfig(props);
        logger.info("Starting connector {}", connectorConfig.name());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return WonderSnowflakeSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> taskProps = new HashMap<>(configProps);
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Shutting down connector {}", connectorConfig.name());
    }

    @Override
    public ConfigDef config() {
        return WonderSnowflakeSinkConnectorConfig.newConfigDef();
    }

    @Override
    public String version() {
        return WonderSnowflakeSinkConnectorConfig.VERSION;
    }
}