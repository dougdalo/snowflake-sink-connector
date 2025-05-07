package br.com.datastreambrasil.v3;

import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.SetParams;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;

@SuppressWarnings("unchecked")
public class SnowflakeSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeSinkConnector.class);
    private Connection connection;
    private SnowflakeConnection snowflakeConnection;
    private String stageName;
    private String tableName;
    private String ingestTableName;
    private String schemaName;
    private boolean cdcFormat;
    private boolean truncateBeforeBulk;
    private int truncateWhenNoDataAfterSeconds;
    private LocalDateTime lastFlush = LocalDateTime.now();
    private boolean flushHasDeletedRecords;
    private boolean flushHasInsertedRecords;
    private boolean flushHasUpdatedRecords;

    private final List<String> timestampFieldsConvertToSeconds = new ArrayList<>();
    private final List<String> pks = new ArrayList<>();
    private final List<String> ignoreColumns = new ArrayList<>();
    private final Map<String, Map<String, Object>> buffer = new HashMap<>();
    private static final String AFTER = "after";
    private static final String BEFORE = "before";
    private static final String OP = "op";
    private static final String IHTOPIC = "ih_topic";
    private static final String IHOFFSET = "ih_offset";
    private static final String IHPARTITION = "ih_partition";
    private static final String IHOP = "ih_op";
    private static final String IHDATETIME = "ih_datetime";
    private static final String IHBLOCKID = "ih_blockid";
    private Scheduler scheduler;

    private enum debeziumOperation {
        d,
        c,
        u,
        r
    }

    /**
     * This flag is enabled while we are receiving only 'r' operation, so we can copy straight to the target table, don't need
     * ingest table. But if we receive a operation different than 'r', we disable this mode.
     */
    private boolean snapshotRecords = true;
    private boolean snapshotMode = false;

    // quartz constants
    protected static final String KEY_SNOWFLAKE_CONNECTION = "snowflakeConnection";
    protected static final String KEY_SNOWFLAKE_INGEST_TABLE_NAME = "snowflakeIngestTableName";

    private List<String> columnsFinalTable = new ArrayList<>();
    private List<String> columnsIngestTable = new ArrayList<>();

    //redis
    private JedisPooled jedis;
    private String redisKeyDmlOperation;
    private int redisKeyTtlSeconds;

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
            ingestTableName = tableName + "_INGEST";
            schemaName = config.getString(SnowflakeSinkConnector.CFG_SCHEMA_NAME);
            cdcFormat = config.getBoolean(SnowflakeSinkConnector.CFG_PAYLOAD_CDC_FORMAT);
            truncateBeforeBulk = config.getBoolean(SnowflakeSinkConnector.CFG_ALWAYS_TRUNCATE_BEFORE_BULK);
            truncateWhenNoDataAfterSeconds = config
                    .getInt(SnowflakeSinkConnector.CFG_TRUNCATE_WHEN_NODATA_AFTER_SECONDS);
            snapshotMode = !config.getBoolean(SnowflakeSinkConnector.CFG_SNAPSHOT_MODE_DISABLE);

            var disableCleanUpJob = config.getBoolean(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE) || truncateBeforeBulk;
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

            if (map.containsKey(SnowflakeSinkConnector.CFG_IGNORE_COLUMNS)) {
                ignoreColumns.addAll(
                        Arrays.stream(map.get(SnowflakeSinkConnector.CFG_IGNORE_COLUMNS).split(","))
                                .toList());
            }

            if (map.containsKey(SnowflakeSinkConnector.CFG_REDIS_HOST)) {
                jedis = new JedisPooled(config.getString(SnowflakeSinkConnector.CFG_REDIS_HOST), config.getInt(SnowflakeSinkConnector.CFG_REDIS_PORT));
                redisKeyDmlOperation = "snowflake_sink_connector_dml_" + map.get(SnowflakeSinkConnector.CFG_SCHEMA_NAME) + "_" + map.get(SnowflakeSinkConnector.CFG_TABLE_NAME) + "_LOCK";
                redisKeyTtlSeconds = config.getInt(SnowflakeSinkConnector.CFG_REDIS_KEY_TTL_SECONDS);
            }

            // init connection
            var properties = new Properties();
            properties.put("user", map.get(SnowflakeSinkConnector.CFG_USER));
            properties.put("password", map.get(SnowflakeSinkConnector.CFG_PASSWORD));
            connection = DriverManager.getConnection(map.get(SnowflakeSinkConnector.CFG_URL), properties);
            snowflakeConnection = connection.unwrap(SnowflakeConnection.class);

            //fill columns
            columnsFinalTable = getColumnsFromMetadata(tableName);
            if (!truncateBeforeBulk) {
                columnsIngestTable = getColumnsFromMetadata(ingestTableName);
            }

            // job quartz config
            if (!disableCleanUpJob) {
                var jobData = new HashMap<String, Object>();
                jobData.put(KEY_SNOWFLAKE_CONNECTION, connection);
                jobData.put(SnowflakeSinkTask.KEY_SNOWFLAKE_INGEST_TABLE_NAME, ingestTableName);
                jobData.put(SnowflakeSinkConnector.CFG_JOB_CLEANUP_HOURS, intervalHoursCleanup);

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
            } else {
                LOGGER.warn("Cleanup job is disabled");
            }

        } catch (Throwable e) {
            LOGGER.error("Error while starting Snowflake connector", e);
            throw new RuntimeException("Error while starting Snowflake connector", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        try {
            for (SinkRecord record : collection) {

                if (record.topic() == null || record.kafkaPartition() == null) {
                    LOGGER.error("Null values for topic or kafkaPartition. Topic {}, KafkaPartition {}", record.topic(),
                            record.kafkaPartition());
                    throw new RuntimeException("Null values for topic or kafkaPartition");
                }

                var mapValue = (Map<String, Object>) record.value();

                if (cdcFormat) {
                    addRecordUsingCDCFormat(mapValue, record);
                } else {
                    addRecordUsingPlainFormat(mapValue, record);
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Error while putting records", e);
            throw new RuntimeException("Error while putting records", e);
        }
    }

    /**
     * Used when the data is ingested not from cdc but manually in the kafka, so we don't have the debezium format
     */
    private void addRecordUsingPlainFormat(Map<String, Object> mapValue, SinkRecord record) {
        snapshotRecords = false;
        var mapCaseInsensitive = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        mapCaseInsensitive.putAll(mapValue);

        // topic,partition,offset,operation
        mapCaseInsensitive.put(IHTOPIC, record.topic());
        mapCaseInsensitive.put(IHPARTITION, record.kafkaPartition());
        mapCaseInsensitive.put(IHOFFSET, String.valueOf(record.kafkaOffset()));
        mapCaseInsensitive.put(IHOP, debeziumOperation.c.toString());
        mapCaseInsensitive.put(IHDATETIME, LocalDateTime.now(ZoneOffset.UTC));

        buffer.put(extractPK(mapCaseInsensitive), mapCaseInsensitive);
    }

    private void addRecordUsingCDCFormat(Map<String, Object> mapValue, SinkRecord record) {

        validateFieldOnMap(OP, mapValue);
        var op = mapValue.get(OP);
        snapshotRecords = op.equals(debeziumOperation.r.toString());

        Map<String, Object> mapPayloadAfterBefore;
        if (op.equals(debeziumOperation.d.toString())) {
            validateFieldOnMap(BEFORE, mapValue);
            mapPayloadAfterBefore = (Map<String, Object>) mapValue.get(BEFORE);
        } else {
            validateFieldOnMap(AFTER, mapValue);
            mapPayloadAfterBefore = (Map<String, Object>) mapValue.get(AFTER);
        }

        // topic,partition,offset,operation
        mapPayloadAfterBefore.put(IHTOPIC, record.topic());
        mapPayloadAfterBefore.put(IHPARTITION, record.kafkaPartition());
        mapPayloadAfterBefore.put(IHOFFSET, String.valueOf(record.kafkaOffset()));
        mapPayloadAfterBefore.put(IHOP, String.valueOf(mapValue.get(OP)));
        mapPayloadAfterBefore.put(IHDATETIME, LocalDateTime.now(ZoneOffset.UTC));

        var mapCaseInsensitive = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        mapCaseInsensitive.putAll(mapPayloadAfterBefore);

        buffer.put(extractPK(mapCaseInsensitive), mapCaseInsensitive);
    }

    private String extractPK(Map<String, Object> mapCaseInsensitive) {
        var buildPK = new StringBuilder();
        for (String pk : pks) {
            if (mapCaseInsensitive.containsKey(pk)) {
                buildPK.append(pk).append(":").append(mapCaseInsensitive.get(pk).toString()).append(",");
            }
        }

        return buildPK.toString();
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

        var useSnapshot = (snapshotRecords && snapshotMode) || truncateBeforeBulk;

        var startTime = System.currentTimeMillis();
        if (buffer.isEmpty()) {
            return;
        }

        var destFileName = UUID.randomUUID().toString();
        try {
            LOGGER.debug("Preparing to send {} records from buffer. To stage {} and table {}", buffer.size(), stageName,
                    tableName);

            /*
             * truncate if truncateBeforeFlush is true and last flush was more than 30
             * minutes ago
             */
            var minutesLastFlush = ChronoUnit.MINUTES.between(lastFlush, LocalDateTime.now());
            if (truncateBeforeBulk && minutesLastFlush > (truncateWhenNoDataAfterSeconds / 60)) {
                try (var stmt = connection.createStatement()) {
                    String truncateTable = String.format("TRUNCATE TABLE %s", tableName);
                    LOGGER.debug("Truncating table: {}", truncateTable);
                    stmt.executeUpdate(truncateTable);
                } catch (Throwable e) {
                    LOGGER.error("Error while truncating table", e);
                    throw new RuntimeException("Error while truncating table", e);
                }
            }

            var columnsFromMetadata = useSnapshot ? columnsFinalTable : columnsIngestTable;
            var blockID = UUID.randomUUID().toString();
            var startTimeMain = System.currentTimeMillis();
            try (var csvToInsert = prepareOrderedColumnsBasedOnTargetTable(blockID, columnsFromMetadata);
                 var inputStream = new ByteArrayInputStream(csvToInsert.toByteArray())) {

                var startTimeUpload = System.currentTimeMillis();
                snowflakeConnection.uploadStream(stageName, "/", inputStream,
                        destFileName, true);
                var endTimeUpload = System.currentTimeMillis();
                LOGGER.debug("Uploaded {} records in {} ms", buffer.size(), endTimeUpload - startTimeUpload);

                var startTimeStatement = System.currentTimeMillis();
                try (var stmt = connection.createStatement()) {

                    if (useSnapshot) {

                        //copy everything to ingest
                        String copyInto = String.format("COPY INTO %s (%s) FROM @%s/%s.gz PURGE = TRUE", tableName, String.join(",", columnsFromMetadata),
                                stageName, destFileName);
                        LOGGER.debug("Copying statement to final table: {}", copyInto);
                        stmt.executeUpdate(copyInto);

                    } else {

                        //copy everything to ingest
                        String copyInto = String.format("COPY INTO %s (%s) FROM @%s/%s.gz PURGE = TRUE", ingestTableName, String.join(",", columnsFromMetadata),
                                stageName, destFileName);
                        LOGGER.debug("Copying statement to ingest table: {}", copyInto);
                        stmt.executeUpdate(copyInto);


                        if (flushHasInsertedRecords) {
                            //insert in final table
                            String insertIntoFinalTable = String.format(
                                    "INSERT INTO %s SELECT * EXCLUDE (%s) FROM %s WHERE ih_blockid = '%s' and ih_op = 'c'",
                                    tableName, buildExcludeColumns(), ingestTableName, blockID);
                            LOGGER.debug("Inserting statement to final table: {}", insertIntoFinalTable);
                            stmt.executeUpdate(insertIntoFinalTable);
                        }

                        //delete from final table
                        if (flushHasDeletedRecords) {
                            waitForRedisLock();
                            String deleteFromFinalTable = String.format(
                                    "DELETE FROM %s as final USING (SELECT %s FROM %s WHERE ih_blockid = '%s' and ih_op = 'd') AS ingest WHERE %s",
                                    tableName, String.join(",", pks), ingestTableName, blockID,
                                    buildPkWhereClause(pks));
                            LOGGER.debug("Deleting statement from final table: {}", deleteFromFinalTable);
                            stmt.executeUpdate(deleteFromFinalTable);
                            releaseRedisLock();
                        }

                        if (flushHasUpdatedRecords) {
                            //update in final table
                            waitForRedisLock();
                            String updateFinalTable = String.format(
                                    "UPDATE %s as final SET %s FROM (SELECT * EXCLUDE (%s) FROM %s WHERE ih_blockid = '%s' and ih_op = 'u') AS ingest WHERE %s",
                                    tableName, buildUpdateColumns(), buildExcludeColumns(), ingestTableName, blockID,
                                    buildPkWhereClause(pks));
                            LOGGER.debug("Updating statement to final table: {}", updateFinalTable);
                            stmt.executeUpdate(updateFinalTable);
                            releaseRedisLock();
                        }
                    }
                    var endTimeStatement = System.currentTimeMillis();
                    LOGGER.debug("Executed statement in {} ms", endTimeStatement - startTimeStatement);

                } catch (SQLException e) {
                    throw new RuntimeException("Error executing operations", e);
                }
            }
            var endTimeMain = System.currentTimeMillis();
            LOGGER.debug("Process records took {} ms", endTimeMain - startTimeMain);

        } catch (Throwable e) {
            LOGGER.error("Error while flushing Snowflake connector", e);
            throw new RuntimeException("Error while flushing", e);
        } finally {
            var endTime = System.currentTimeMillis();
            LOGGER.debug("Flushed {} records in {} ms", buffer.size(), endTime - startTime);
            buffer.clear();
            lastFlush = LocalDateTime.now();
        }
    }

    private void waitForRedisLock() {
        if (jedis != null) {
            String lockStatus = null;
            while (!"OK".equalsIgnoreCase(lockStatus)) {
                lockStatus = jedis.set(redisKeyDmlOperation, "1", new SetParams().nx().ex(redisKeyTtlSeconds));
                LOGGER.warn("Lock on redis key {} not acquired, since status is {} and we expect OK. Likely other task is using it. Will try again in some seconds ...", redisKeyDmlOperation, lockStatus);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.error("Error while sleeping for redis lock", e);
                    throw new RuntimeException("Error while sleeping for redis lock", e);
                }
            }

            LOGGER.debug("Redis lock acquired on key {}", redisKeyDmlOperation);
        } else {
            LOGGER.debug("Jedis is not configured, so we will not use redis lock");
        }
    }

    private void releaseRedisLock() {
        if (jedis != null) {
            jedis.del(redisKeyDmlOperation.getBytes());
            LOGGER.debug("Redis lock released on key {}", redisKeyDmlOperation);
        } else {
            LOGGER.debug("Jedis is not configured, so we can't release lock");
        }
    }

    private String buildUpdateColumns() {
        var columns = new ArrayList<String>();
        for (String column : columnsFinalTable) {
            if (!pks.contains(column)) {
                columns.add(String.format("final.%s = ingest.%s", column, column));
            }
        }
        return String.join(",", columns);
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

        LOGGER.debug("Columns mapped from target table: {}", String.join(",", columnsNoDuplicate));


        return columnsNoDuplicate;
    }

    private String buildExcludeColumns() {
        return String.join(",", IHPARTITION, IHDATETIME, IHBLOCKID, IHOFFSET, IHOP, IHTOPIC);
    }

    private String buildPkWhereClause(List<String> pks) {
        return pks.stream()
                .map(col -> String.format("%s.%s = %s.%s", "final", col, "ingest", col))
                .reduce((a, b) -> a + " and " + b).orElseThrow();
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

        if (jedis != null) {
            jedis.close();
        }
    }

    private void validateFieldOnMap(String fieldToValidate, Map<String, ?> map) {
        if (!map.containsKey(fieldToValidate) || map.get(fieldToValidate) == null) {
            LOGGER.error("Key [{}] is missing or null on json", fieldToValidate);
            throw new RuntimeException("missing or null key [" + fieldToValidate + "] on json");
        }
    }

    private ByteArrayOutputStream prepareOrderedColumnsBasedOnTargetTable(String blockID, List<String> columnsFromTable) throws Throwable {

        var startTime = System.currentTimeMillis();
        var csvInMemory = new ByteArrayOutputStream();
        var stringBuilder = new StringBuilder();

        flushHasDeletedRecords = false;
        flushHasUpdatedRecords = false;
        flushHasInsertedRecords = false;

        for (var recordInBuffer : buffer.values()) {
            var count = 0;
            var op = recordInBuffer.get(IHOP).toString();

            if (!flushHasDeletedRecords && debeziumOperation.d.toString().equalsIgnoreCase(op)) {
                flushHasDeletedRecords = true;
            }

            if (!flushHasInsertedRecords && debeziumOperation.c.toString().equalsIgnoreCase(op)) {
                flushHasInsertedRecords = true;
            }

            if (!flushHasUpdatedRecords && debeziumOperation.u.toString().equalsIgnoreCase(op)) {
                flushHasUpdatedRecords = true;
            }

            for (String columnFromSnowflakeTable : columnsFromTable) {
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
                        stringBuilder.append(strBuffer);
                    }
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHBLOCKID)) {
                    var strBuffer = "\"" + blockID + "\"";
                    stringBuilder.append(strBuffer);
                } else {
                    LOGGER.warn("Column {} not found on buffer, inserted empty value", columnFromSnowflakeTable);
                }

                if (count < columnsFromTable.size() - 1) {
                    stringBuilder.append(",");
                }

                count++;
            }


            stringBuilder.append("\n");
        }

        if (!stringBuilder.isEmpty()) {
            csvInMemory.write(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
        }
        var endTime = System.currentTimeMillis();
        LOGGER.debug("Prepared csv in memory in {} ms", endTime - startTime);
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