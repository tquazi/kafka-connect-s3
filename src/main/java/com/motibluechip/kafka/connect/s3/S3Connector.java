package com.motibluechip.kafka.connect.s3;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import com.motibluechip.kafka.connect.utils.StringUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import java.util.*;


/**
 *
 * @author Tabrez Quazi
 */

public class S3Connector extends SourceConnector {
    public static final String S3_BUCKET_NAME = "s3.bucket.name";
    public static final String S3_ACCESS_KEY = "s3.access.key";
    public static final String S3_SECRET_KEY = "s3.secret.key";
    public static final String S3_REGION = "s3.region";

    public static final String CHCK_DIR_MS = "check.dir.ms";
    public static final String SCHEMA_NAME = "schema.name";
    public static final String TOPIC = "topic";

    private String bucket_name;
    private String access_key;
    private String secret_key;
    private String region;
    private String check_dir_ms;
    private String schema_name;
    private String topic;

	private static final ConfigDef CONFIG_DEF = new ConfigDef();


    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }


    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {
    	bucket_name = props.get(S3_BUCKET_NAME);
        if (bucket_name == null || bucket_name.isEmpty())
            throw new ConnectException("missing s3.bucket.name");

        access_key = props.get(S3_ACCESS_KEY);
        if (access_key == null || access_key.isEmpty())
            throw new ConnectException("missing s3.access.key");

        secret_key = props.get(S3_SECRET_KEY);
        if (secret_key == null || secret_key.isEmpty())
            throw new ConnectException("missing s3.secret.key");

        region = props.get(S3_REGION);
        if (region == null || region.isEmpty())
            throw new ConnectException("missing s3.region");


        schema_name = props.get(SCHEMA_NAME);
        if (schema_name == null || schema_name.isEmpty())
            throw new ConnectException("missing schema.name");

        topic = props.get(TOPIC);
        if (topic == null || topic.isEmpty())
            throw new ConnectException("missing topic");


        check_dir_ms = props.get(CHCK_DIR_MS);
        if (check_dir_ms == null || check_dir_ms.isEmpty())
            check_dir_ms = "1000";
    }


    /**
     * Returns the Task implementation for this Connector.
     *
     * @return tha Task implementation Class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return S3Task.class;
    }


    /**
     * Returns a set of configurations for the Tasks based on the current configuration.
     * It always creates a single set of configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for the Task
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        List<String> dirs = Arrays.asList(bucket_name.split(","));
        for (int i = 0; i < dirs.size(); i++) {
            Map<String, String> config = new HashMap<>();
            config.put(CHCK_DIR_MS, check_dir_ms);
            config.put(SCHEMA_NAME, schema_name);
            config.put(TOPIC, topic);
            config.put(S3_BUCKET_NAME, dirs.get(i));
            config.put(S3_ACCESS_KEY, access_key);
            config.put(S3_SECRET_KEY, secret_key);
            config.put(S3_REGION, region);

            configs.add(config);
        }
        return configs;
    }


    /**
     * Stop this connector.
     */
    @Override
    public void stop() {

    }

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public Config validate(Map<String, String> connectorConfigs) {
		ConfigDef configDef = config();
		List<ConfigValue> configValues = configDef.validate(connectorConfigs);
		return new Config(configValues);
	}

}
