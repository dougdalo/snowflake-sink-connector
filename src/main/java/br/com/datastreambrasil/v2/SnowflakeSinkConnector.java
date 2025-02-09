package br.com.datastreambrasil.v2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class SnowflakeSinkConnector extends SinkConnector {

    protected static final String VERSION = "1.0.8";
    protected static final String CFG_STAGE_NAME = "stage";
    protected static final String CFG_TABLE_NAME = "table";
    protected static final String CFG_TIMESTAMP_FIELDS_CONVERT_SECONDS = "timestamp_fields_convert_seconds";
    protected static final String CFG_SCHEMA_NAME = "schema";
    protected static final String CFG_URL = "url";
    protected static final String CFG_USER = "user";
    protected static final String CFG_PASSWORD = "password";
    protected static final String CFG_PK = "pk";
    protected static final String CFG_JOB_CLEANUP_HOURS = "job_cleanup_hours";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CFG_STAGE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "Stage name to write files")
            .define(CFG_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "URL to snowflake connection")
            .define(CFG_USER, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "User to snowflake connection")
            .define(CFG_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "Password to snowflake connection")
            .define(CFG_TABLE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "Target table to copy data into")
            .define(CFG_TIMESTAMP_FIELDS_CONVERT_SECONDS, ConfigDef.Type.LIST, null, ConfigDef.Importance.MEDIUM,
                    "List of timestamp fields we should convert to seconds")
            .define(CFG_PK, ConfigDef.Type.LIST, null, ConfigDef.Importance.MEDIUM,
                    "List of primary keys to be used")
            .define(CFG_JOB_CLEANUP_HOURS, ConfigDef.Type.INT, 4, ConfigDef.Importance.MEDIUM,
                    "Interval in hours to cleanup old data")
            .define(CFG_SCHEMA_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "Target schema to copy data into");

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> map) {
        this.props = map;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SnowflakeSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return VERSION;
    }
}
