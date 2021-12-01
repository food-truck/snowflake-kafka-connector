package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import org.apache.kafka.common.config.Config;
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
    private final Logger LOGGER = LoggerFactory.getLogger(WonderSnowflakeSinkConnector.class);

    private Map<String, String> configProps;
    private WonderSnowflakeSinkConnectorConfig connectorConfig;
    private SnowflakeConnectionService conn;

    public WonderSnowflakeSinkConnector() {

    }

    @Override
    public void start(Map<String, String> props) {
        configProps = new HashMap<>(props);
        connectorConfig = new WonderSnowflakeSinkConnectorConfig(props);

        initSnowflakeConfig(configProps);

        logger.info("Starting connector {}", connectorConfig.name());
    }

    private void initSnowflakeConfig(Map<String, String> config) {
        SnowflakeSinkConnectorConfig.setDefaultValues(config);

        Utils.validateConfig(config);

        // modify invalid connector name
        Utils.convertAppName(config);

        // enable proxy
        Utils.enableJVMProxy(config);
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
    public Config validate(Map<String, String> connectorConfigs) {
        LOGGER.debug("Validating connector Config: Start");
        // cross-fields validation here
        Config result = super.validate(connectorConfigs);

        // Validate ensure that url, user, db, schema, private key exist in config and is not empty
        // and there is no single field validation error
        if (!Utils.isSingleFieldValid(result)) {
            return result;
        }

        // Verify proxy config is valid
        try {
            Utils.validateProxySetting(connectorConfigs);
        } catch (SnowflakeKafkaConnectorException e) {
            LOGGER.error("Error validating proxy parameters:{}", e.getMessage());
            switch (e.getCode()) {
                case "0022":
                    Utils.updateConfigErrorMessage(
                            result,
                            SnowflakeSinkConnectorConfig.JVM_PROXY_HOST,
                            ": proxy host and port must be provided together");
                    Utils.updateConfigErrorMessage(
                            result,
                            SnowflakeSinkConnectorConfig.JVM_PROXY_PORT,
                            ": proxy host and port must be provided together");
                case "0023":
                    Utils.updateConfigErrorMessage(
                            result,
                            SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME,
                            ": proxy username and password must be provided together");
                    Utils.updateConfigErrorMessage(
                            result,
                            SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD,
                            ": proxy username and password must be provided together");
            }
        }
        // If private key or private key passphrase is provided through file, skip validation
        if (connectorConfigs.getOrDefault(Utils.SF_PRIVATE_KEY, "").contains("${file:")
                || connectorConfigs.getOrDefault(Utils.PRIVATE_KEY_PASSPHRASE, "").contains("${file:"))
            return result;

        // We don't validate name, since it is not included in the return value
        // so just put a test connector here
        connectorConfigs.put(Utils.NAME, "TEST_CONNECTOR");
        SnowflakeConnectionService testConnection;
        try {
            testConnection =
                    SnowflakeConnectionServiceFactory.builder().setProperties(connectorConfigs).build();
        } catch (SnowflakeKafkaConnectorException e) {
            LOGGER.error(
                    "Validate: Error connecting to snowflake:{}, errorCode:{}", e.getMessage(), e.getCode());
            // Since url, user, db, schema, exist in config and is not empty,
            // the exceptions here would be invalid URL, and cannot connect, and no private key
            switch (e.getCode()) {
                case "1001":
                    // Could be caused by invalid url, invalid user name, invalid password.
                    Utils.updateConfigErrorMessage(result, Utils.SF_URL, ": Cannot connect to Snowflake");
                    Utils.updateConfigErrorMessage(
                            result, Utils.SF_PRIVATE_KEY, ": Cannot connect to Snowflake");
                    Utils.updateConfigErrorMessage(result, Utils.SF_USER, ": Cannot connect to Snowflake");
                    break;
                case "0007":
                    Utils.updateConfigErrorMessage(result, Utils.SF_URL, " is not a valid snowflake url");
                    break;
                case "0018":
                    Utils.updateConfigErrorMessage(result, Utils.PRIVATE_KEY_PASSPHRASE, " is not valid");
                    Utils.updateConfigErrorMessage(result, Utils.SF_PRIVATE_KEY, " is not valid");
                    break;
                case "0013":
                    Utils.updateConfigErrorMessage(result, Utils.SF_PRIVATE_KEY, " must be non-empty");
                    break;
                case "0002":
                    Utils.updateConfigErrorMessage(
                            result, Utils.SF_PRIVATE_KEY, " must be a valid PEM RSA private key");
                    break;
                default:
                    throw e; // Shouldn't reach here, so crash.
            }
            return result;
        }

        try {
            testConnection.databaseExists(connectorConfigs.get(Utils.SF_DATABASE));
        } catch (SnowflakeKafkaConnectorException e) {
            LOGGER.error("Validate Error msg:{}, errorCode:{}", e.getMessage(), e.getCode());
            if (e.getCode().equals("2001")) {
                Utils.updateConfigErrorMessage(result, Utils.SF_DATABASE, " database does not exist");
            } else {
                throw e;
            }
            return result;
        }

        try {
            testConnection.schemaExists(connectorConfigs.get(Utils.SF_SCHEMA));
        } catch (SnowflakeKafkaConnectorException e) {
            LOGGER.error("Validate Error msg:{}, errorCode:{}", e.getMessage(), e.getCode());
            if (e.getCode().equals("2001")) {
                Utils.updateConfigErrorMessage(result, Utils.SF_SCHEMA, " schema does not exist");
            } else {
                throw e;
            }
            return result;
        }

        LOGGER.info("Validated config with no error");
        return result;
    }

    @Override
    public String version() {
        return WonderSnowflakeSinkConnectorConfig.VERSION;
    }
}