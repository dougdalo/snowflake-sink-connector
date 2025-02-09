package br.com.datastreambrasil.v2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class SnowflakeSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeSinkConnector.class);
    private Connection connection;
    private SnowflakeConnection snowflakeConnection;
    private String stageName;
    private String tableName;
    private String schemaName;
    private final List<String> timestampFieldsConvertToSeconds = new ArrayList<>();
    private final List<String> pks = new ArrayList<>();
    private final Collection<Map<String, Object>> buffer = new ArrayList<>();
    private static final String PAYLOAD = "payload";
    private static final String AFTER = "after";
    private static final String BEFORE = "before";
    private static final String OP = "op";
    private static final String IHTOPIC = "ih_topic";
    private static final String IHOFFSET = "ih_offset";
    private static final String IHPARTITION = "ih_partition";
    private static final String IHOP = "ih_op";
    private static final String DELETE = "d";
    private Scheduler scheduler;

    // quartz constants
    protected static final String KEY_SNOWFLAKE_CONNECTION = "snowflakeConnection";

    @Override
    public String version() {
        return SnowflakeSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        try {
            AbstractConfig config = new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, map);

            // init configs
            stageName = config.getString(SnowflakeSinkConnector.CFG_STAGE_NAME);
            tableName = config.getString(SnowflakeSinkConnector.CFG_TABLE_NAME);
            schemaName = config.getString(SnowflakeSinkConnector.CFG_SCHEMA_NAME);
            var intervalHoursCleanup = config.getInt(SnowflakeSinkConnector.CFG_JOB_CLEANUP_HOURS);

            if (map.containsKey(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT_SECONDS)) {
                timestampFieldsConvertToSeconds.addAll(
                        Arrays.stream(map.get(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT_SECONDS).split(","))
                                .toList());
            }

            if (map.containsKey(SnowflakeSinkConnector.CFG_PK)) {
                pks.addAll(
                        Arrays.stream(map.get(SnowflakeSinkConnector.CFG_PK).split(","))
                                .toList());
            }

            // init connection
            var properties = new Properties();
            properties.put("user", map.get(SnowflakeSinkConnector.CFG_USER));
            properties.put("password", map.get(SnowflakeSinkConnector.CFG_PASSWORD));
            connection = DriverManager.getConnection(map.get(SnowflakeSinkConnector.CFG_URL), properties);
            snowflakeConnection = connection.unwrap(SnowflakeConnection.class);

            // job quartz config
            var jobData = new HashMap<String, Object>();
            jobData.put(KEY_SNOWFLAKE_CONNECTION, connection);
            jobData.put(SnowflakeSinkConnector.CFG_TABLE_NAME, tableName);
            jobData.put(SnowflakeSinkConnector.CFG_PK, pks);

            var uuid = UUID.randomUUID().toString();
            var props = new Properties();
            props.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, "cleanup_" + uuid);
            props.setProperty("org.quartz.threadPool.threadCount", "1");

            var schedulerFactory = new StdSchedulerFactory(props);
            scheduler = schedulerFactory.getScheduler();
            var job = JobBuilder.newJob(CleanupJob.class).withIdentity("cleanupjob")
                    .setJobData(new JobDataMap(jobData))
                    .build();
            var trigger = TriggerBuilder.newTrigger().withIdentity("trigger_cleanupjob")
                    .withSchedule(SimpleScheduleBuilder.repeatHourlyForever(intervalHoursCleanup))
                    .build();
            scheduler.scheduleJob(job, trigger);
            scheduler.start();

        } catch (Throwable e) {
            LOGGER.error("Error while starting Snowflake connector", e);
            throw new RuntimeException("Error while starting Snowflake connector", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        try {
            for (SinkRecord record : collection) {
                var mapValue = (Map<String, Object>) record.value();

                validateFieldOnMap(PAYLOAD, mapValue);
                var mapPayload = (Map<String, Object>) mapValue.get(PAYLOAD);

                validateFieldOnMap(OP, mapPayload);
                var op = mapPayload.get(OP);
                Map<String, Object> mapPayloadAfterBefore;
                if (op.equals(DELETE)) {
                    validateFieldOnMap(BEFORE, mapPayload);
                    mapPayloadAfterBefore = (Map<String, Object>) mapPayload.get(BEFORE);
                } else {
                    validateFieldOnMap(AFTER, mapPayload);
                    mapPayloadAfterBefore = (Map<String, Object>) mapPayload.get(AFTER);
                }

                if (record.topic() == null || record.kafkaPartition() == null) {
                    LOGGER.error("Null values for topic or kafkaPartition. Topic {}, KafkaPartition {}", record.topic(),
                            record.kafkaPartition());
                    throw new RuntimeException("Null values for topic or kafkaPartition");
                }

                // topic,partition,offset,operation
                mapPayloadAfterBefore.put(IHTOPIC, record.topic());
                mapPayloadAfterBefore.put(IHPARTITION, record.kafkaPartition());
                mapPayloadAfterBefore.put(IHOFFSET, String.valueOf(record.kafkaOffset()));
                mapPayloadAfterBefore.put(IHOP, String.valueOf(mapPayload.get(OP)));

                var mapCaseInsensitive = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                mapCaseInsensitive.putAll(mapPayloadAfterBefore);

                buffer.add(mapCaseInsensitive);
            }
        } catch (Throwable e) {
            LOGGER.error("Error while putting records", e);
            throw new RuntimeException("Error while putting records", e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

        if (buffer.isEmpty()) {
            return;
        }

        var destFileName = UUID.randomUUID().toString();
        try {
            LOGGER.debug("Preparing to send {} records from buffer. To stage {} and table {}", buffer.size(), stageName,
                    tableName);

            try (var csvToInsert = prepareOrderedColumnsBasedOnTargetTable();
                    var inputStream = new ByteArrayInputStream(csvToInsert.toByteArray())) {
                snowflakeConnection.uploadStream(stageName, "/", inputStream,
                        destFileName, true);
                try (var stmt = connection.createStatement()) {
                    String copyInto = String.format("COPY INTO %s FROM @%s/%s.gz PURGE = TRUE", tableName, stageName,
                            destFileName);
                    LOGGER.debug("Copying statement: {}", copyInto);
                    stmt.executeUpdate(copyInto);
                }
            }

        } catch (Throwable e) {
            try {
                try (var stmt = connection.createStatement()) {
                    String removeFileFromStage = String.format("REMOVE @%s/%s.gz", stageName, destFileName);
                    stmt.execute(removeFileFromStage);
                }
            } catch (Throwable e2) {
                throw new RuntimeException("Error while removing file [" + destFileName + "] from stage " + stageName,
                        e2);
            }

            LOGGER.error("Error while flushing Snowflake connector", e);
            throw new RuntimeException("Error while flushing", e);
        } finally {
            buffer.clear();
        }
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            try {
                scheduler.shutdown();
            } catch (SchedulerException e) {
                LOGGER.error("Can not shutdown quartz scheduler", e);
            }
        }
    }

    private void validateFieldOnMap(String fieldToValidate, Map<String, ?> map) {
        if (!map.containsKey(fieldToValidate) || map.get(fieldToValidate) == null) {
            LOGGER.error("Key [{}] is missing or null on json", fieldToValidate);
            throw new RuntimeException("missing or null key [" + fieldToValidate + "] on json");
        }
    }

    private ByteArrayOutputStream prepareOrderedColumnsBasedOnTargetTable() throws Throwable {
        var metadata = connection.getMetaData();

        var columnsFromTable = new ArrayList<String>();
        try (var rsColumns = metadata.getColumns(null, schemaName.toUpperCase(), tableName.toUpperCase(), null)) {
            while (rsColumns.next()) {
                columnsFromTable.add(rsColumns.getString("COLUMN_NAME").toUpperCase());
            }
        }
        if (columnsFromTable.isEmpty()) {
            throw new RuntimeException(
                    "Empty columns returned from target table " + tableName + ", schema " + schemaName);
        }

        LOGGER.debug("Columns mapped from target table: {}", String.join(",", columnsFromTable));

        var csvInMemory = new ByteArrayOutputStream();

        for (var recordInBuffer : buffer) {
            for (int i = 0; i < columnsFromTable.size(); i++) {
                var columnFromSnowflakeTable = columnsFromTable.get(i);
                if (recordInBuffer.containsKey(columnFromSnowflakeTable)) {
                    var valueFromRecord = recordInBuffer.get(columnFromSnowflakeTable);

                    if (valueFromRecord != null
                            && containsAny(columnFromSnowflakeTable, timestampFieldsConvertToSeconds)) {
                        var valueFromRecordAsLong = (long) valueFromRecord;
                        valueFromRecord = LocalDateTime.ofInstant(Instant.ofEpochMilli(valueFromRecordAsLong),
                                TimeZone.getDefault().toZoneId()).toString();
                    }

                    if (valueFromRecord != null) {
                        var strBuffer = "\"" + valueFromRecord + "\"";
                        csvInMemory.writeBytes(strBuffer.getBytes());
                    }
                } else {
                    LOGGER.warn("Column {} not found on buffer, inserted empty value", columnFromSnowflakeTable);
                }

                if (i < columnsFromTable.size() - 1) {
                    csvInMemory.writeBytes(",".getBytes());
                }
            }

            csvInMemory.writeBytes("\n".getBytes());
        }

        return csvInMemory;
    }

    private boolean containsAny(String checkValue, List<String> values) {
        for (String s : values) {
            if (s.trim().equalsIgnoreCase(checkValue.trim())) {
                return true;
            }
        }

        return false;
    }
}