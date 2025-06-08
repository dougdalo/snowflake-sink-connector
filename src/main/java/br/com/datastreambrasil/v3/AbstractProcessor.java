package br.com.datastreambrasil.v3;

import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public abstract class AbstractProcessor {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected Connection connection;
    protected SnowflakeConnection snowflakeConnection;
    protected String stageName;
    protected String tableName;
    protected String ingestTableName;
    protected String schemaName;
    protected List<String> columnsFinalTable = new ArrayList<>();
    protected List<String> columnsIngestTable = new ArrayList<>();
    protected List<String> ignoreColumns = new ArrayList<>();
    protected List<String> timestampFieldsConvert = new ArrayList<>();
    protected List<String> dateFieldsConvert = new ArrayList<>();
    protected List<String> timeFieldsConvert = new ArrayList<>();

    protected static final String AFTER = "after";
    protected static final String BEFORE = "before";
    protected static final String OP = "op";
    protected static final String IHTOPIC = "ih_topic";
    protected static final String IHOFFSET = "ih_offset";
    protected static final String IHPARTITION = "ih_partition";
    protected static final String IHOP = "ih_op";
    protected static final String IHDATETIME = "ih_datetime";
    protected static final String IHBLOCKID = "ih_blockid";

    protected enum debeziumOperation {
        d,
        c,
        u,
        r
    }

    protected final static String INGEST_SUFFIX = "_INGEST";

    protected abstract void extraConfigsOnStart(AbstractConfig config);

    protected abstract void put(Collection<SinkRecord> collection);

    protected abstract void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets);

    protected abstract void stop();

    protected void validateConfigBeforeStart(AbstractConfig config) {
        // Validate that all required configurations are present
    }

    protected void start(AbstractConfig config) {
        stageName = config.getString(SnowflakeSinkConnector.CFG_STAGE_NAME);
        tableName = config.getString(SnowflakeSinkConnector.CFG_TABLE_NAME);
        ingestTableName = tableName + INGEST_SUFFIX;
        schemaName = config.getString(SnowflakeSinkConnector.CFG_SCHEMA_NAME);

        if (config.getString(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT) != null &&
                !config.getString(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT).isEmpty()) {
            timestampFieldsConvert.addAll(
                    Arrays.stream(config.getString(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT).split(","))
                            .toList());
        }

        if (config.getString(SnowflakeSinkConnector.CFG_DATE_FIELDS_CONVERT) != null &&
                !config.getString(SnowflakeSinkConnector.CFG_DATE_FIELDS_CONVERT).isEmpty()) {
            dateFieldsConvert.addAll(
                    Arrays.stream(config.getString(SnowflakeSinkConnector.CFG_DATE_FIELDS_CONVERT).split(","))
                            .toList());
        }

        if (config.getString(SnowflakeSinkConnector.CFG_TIME_FIELDS_CONVERT) != null &&
                !config.getString(SnowflakeSinkConnector.CFG_TIME_FIELDS_CONVERT).isEmpty()) {
            timeFieldsConvert.addAll(
                    Arrays.stream(config.getString(SnowflakeSinkConnector.CFG_TIME_FIELDS_CONVERT).split(","))
                            .toList());
        }

        if (config.getString(SnowflakeSinkConnector.CFG_IGNORE_COLUMNS) != null
                && !config.getString(SnowflakeSinkConnector.CFG_IGNORE_COLUMNS).isEmpty()) {
            ignoreColumns.addAll(Arrays.stream(config.getString(SnowflakeSinkConnector.CFG_IGNORE_COLUMNS)
                    .split(",")).toList());
        }

        try {
            setupSnowflakeConnection(config);
        } catch (SQLException e) {
            logger.error("Error while connecting to snowflake connection", e);
            throw new RuntimeException("Error while connecting to snowflake connection", e);
        }

        try {
            columnsFinalTable = getColumnsFromMetadata(tableName);
            columnsIngestTable = getColumnsFromMetadata(ingestTableName);
        } catch (SQLException e) {
            logger.error("Error while get metadata columns from snowflake", e);
            throw new RuntimeException("Error while get metadata columns from snowflake", e);
        }

        extraConfigsOnStart(config);
    }

    protected void setupSnowflakeConnection(AbstractConfig config) throws SQLException {
        var properties = new Properties();
        properties.put("user", config.getString(SnowflakeSinkConnector.CFG_USER));
        properties.put("password", config.getString(SnowflakeSinkConnector.CFG_PASSWORD));
        connection = DriverManager.getConnection(config.getString(SnowflakeSinkConnector.CFG_URL), properties);
        snowflakeConnection = connection.unwrap(SnowflakeConnection.class);   // using the provided configuration.
    }

    private List<String> getColumnsFromMetadata(String table) throws SQLException {
        var metadata = connection.getMetaData();

        var columnsFromTable = new ArrayList<String>();
        try (var rsColumns = metadata.getColumns(null, schemaName.toUpperCase(), table.toUpperCase(), null)) {
            while (rsColumns.next()) {
                columnsFromTable.add(rsColumns.getString("COLUMN_NAME").toUpperCase());
            }
        }
        if (columnsFromTable.isEmpty()) {
            throw new RuntimeException(
                    "Empty columns returned from target table " + table + ", schema " + schemaName);
        }

        columnsFromTable.removeAll(ignoreColumns);
        //remove duplicated
        var columnsNoDuplicate = columnsFromTable.stream().distinct().toList();

        logger.debug("Columns mapped from target table: {}", String.join(",", columnsNoDuplicate));


        return columnsNoDuplicate;
    }
}
