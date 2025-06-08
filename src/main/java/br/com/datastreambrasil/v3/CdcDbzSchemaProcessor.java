package br.com.datastreambrasil.v3;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.*;
import java.util.*;

/**
 * CdcDbzSchemaProcessor receives SinkRecords using Struct with Schema in CDC Debezium format.
 * This processor will process all operations (snapshot,insert, update, delete) and save on snowflake.
 */
public class CdcDbzSchemaProcessor extends AbstractProcessor {

    private Scheduler scheduler;
    private List<String> pks = new ArrayList<>();
    private Map<String, SnowflakeRecord> buffer = new HashMap<>();
    private boolean snapshotRecords = true;
    private boolean flushHasDeletedRecords;
    private boolean flushHasInsertedRecords;
    private boolean flushHasUpdatedRecords;

    static class SnowflakeRecord {
        private Struct event;
        private String topic;
        private int partition;
        private long offset;
        private String op;
        private LocalDateTime timestamp;
    }

    @Override
    protected void extraConfigsOnStart(AbstractConfig config) {
        try {
            startCleanUpJob(config);
        } catch (SchedulerException e) {
            logger.error("Error starting cleanup job", e);
            throw new RuntimeException("Error starting cleanup job", e);
        }
    }

    @Override
    protected void put(Collection<SinkRecord> collection) {
        try {
            for (SinkRecord record : collection) {

                if (record.topic() == null || record.kafkaPartition() == null) {
                    logger.error("Null values for topic or kafkaPartition. Topic {}, KafkaPartition {}", record.topic(),
                            record.kafkaPartition());
                    throw new RuntimeException("Null values for topic or kafkaPartition");
                }

                pks = extractPK(record);

                var fieldOP = record.valueSchema().field(OP);
                if (fieldOP == null) {
                    logger.error("Field '{}' not found in value schema for record: {}", OP, record);
                    throw new RuntimeException("Field '" + OP + "' not found in value schema");
                }

                if (!(record.value() instanceof Struct valueRecord)) {
                    throw new RuntimeException("Expected value to be a Struct, but got: " + record.value().getClass().getName());
                }

                var valueOP = valueRecord.getString(fieldOP.name());
                if (valueOP == null) {
                    logger.error("Value for field '{}' is null in record: {}", OP, record);
                    throw new RuntimeException("Value for field '" + OP + "' is null");
                }


                snapshotRecords = debeziumOperation.r.toString().equalsIgnoreCase(valueOP);

                var recordToSnowflake = new SnowflakeRecord();
                if (debeziumOperation.d.toString().equalsIgnoreCase(valueOP)) {
                    recordToSnowflake.event = valueRecord.getStruct(BEFORE);
                } else {
                    recordToSnowflake.event = valueRecord.getStruct(AFTER);
                }


                recordToSnowflake.topic = record.topic();
                recordToSnowflake.partition = record.kafkaPartition();
                recordToSnowflake.offset = record.kafkaOffset();
                recordToSnowflake.op = valueOP;
                recordToSnowflake.timestamp = LocalDateTime.now(ZoneOffset.UTC);

                buffer.put(convertPKToStringKey(record), recordToSnowflake);
            }
        } catch (Throwable e) {
            logger.error("Error while putting records", e);
            throw new RuntimeException("Error while putting records", e);
        }
    }

    @Override
    protected void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        var startTime = System.currentTimeMillis();
        if (buffer.isEmpty()) {
            return;
        }

        var destFileName = UUID.randomUUID().toString();
        try {
            logger.debug("Preparing to send {} records from buffer. To stage {} and table {}", buffer.size(), stageName,
                    tableName);

            var columnsFromMetadata = snapshotRecords ? columnsFinalTable : columnsIngestTable;
            var blockID = UUID.randomUUID().toString();
            var startTimeMain = System.currentTimeMillis();
            try (var csvToInsert = prepareOrderedColumnsBasedOnTargetTable(blockID, columnsFromMetadata);
                 var inputStream = new ByteArrayInputStream(csvToInsert.toByteArray())) {

                var startTimeUpload = System.currentTimeMillis();
                snowflakeConnection.uploadStream(stageName, "/", inputStream,
                        destFileName, true);
                var endTimeUpload = System.currentTimeMillis();
                logger.debug("Uploaded {} records in {} ms", buffer.size(), endTimeUpload - startTimeUpload);

                var startTimeStatement = System.currentTimeMillis();
                try (var stmt = connection.createStatement()) {

                    if (snapshotRecords) {

                        //copy everything to ingest
                        String copyInto = String.format("COPY INTO %s (%s) FROM @%s/%s.gz PURGE = TRUE", tableName, String.join(",", columnsFromMetadata),
                                stageName, destFileName);
                        logger.debug("Copying statement to final table: {}", copyInto);
                        stmt.executeUpdate(copyInto);

                    } else {

                        //copy everything to ingest
                        String copyInto = String.format("COPY INTO %s (%s) FROM @%s/%s.gz PURGE = TRUE", ingestTableName, String.join(",", columnsFromMetadata),
                                stageName, destFileName);
                        logger.debug("Copying statement to ingest table: {}", copyInto);
                        stmt.executeUpdate(copyInto);


                        if (flushHasInsertedRecords) {
                            //insert in final table
                            String insertIntoFinalTable = String.format(
                                    "INSERT INTO %s SELECT * EXCLUDE (%s) FROM %s WHERE ih_blockid = '%s' and ih_op = 'c'",
                                    tableName, buildExcludeColumns(), ingestTableName, blockID);
                            logger.debug("Inserting statement to final table: {}", insertIntoFinalTable);
                            stmt.executeUpdate(insertIntoFinalTable);
                        }

                        //delete from final table
                        if (flushHasDeletedRecords) {
                            String deleteFromFinalTable = String.format(
                                    "DELETE FROM %s as final USING (SELECT %s FROM %s WHERE ih_blockid = '%s' and ih_op = 'd') AS ingest WHERE %s",
                                    tableName, String.join(",", pks), ingestTableName, blockID,
                                    buildPkWhereClause(pks));
                            logger.debug("Deleting statement from final table: {}", deleteFromFinalTable);
                            stmt.executeUpdate(deleteFromFinalTable);
                        }

                        if (flushHasUpdatedRecords) {
                            //update in final table
                            String updateFinalTable = String.format(
                                    "UPDATE %s as final SET %s FROM (SELECT * EXCLUDE (%s) FROM %s WHERE ih_blockid = '%s' and ih_op = 'u') AS ingest WHERE %s",
                                    tableName, buildUpdateColumns(), buildExcludeColumns(), ingestTableName, blockID,
                                    buildPkWhereClause(pks));
                            logger.debug("Updating statement to final table: {}", updateFinalTable);
                            stmt.executeUpdate(updateFinalTable);
                        }
                    }
                    var endTimeStatement = System.currentTimeMillis();
                    logger.debug("Executed statement in {} ms", endTimeStatement - startTimeStatement);

                } catch (SQLException e) {
                    throw new RuntimeException("Error executing operations", e);
                }
            }
            var endTimeMain = System.currentTimeMillis();
            logger.debug("Process records took {} ms", endTimeMain - startTimeMain);

        } catch (Throwable e) {
            logger.error("Error while flushing Snowflake connector", e);
            throw new RuntimeException("Error while flushing", e);
        } finally {
            var endTime = System.currentTimeMillis();
            logger.debug("Flushed {} records in {} ms", buffer.size(), endTime - startTime);
            buffer.clear();
        }
    }

    @Override
    protected void stop() {
        if (scheduler != null) {
            try {
                scheduler.shutdown();
            } catch (SchedulerException e) {
                logger.error("Can not shutdown quartz scheduler", e);
            }
        }
    }

    private void startCleanUpJob(AbstractConfig config) throws SchedulerException {

        var disableCleanUpJob = config.getBoolean(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE);

        if (disableCleanUpJob) {
            logger.warn("Cleanup job is disabled, skipping job creation.");
            return;
        }

        var intervalHoursCleanup = config.getInt(SnowflakeSinkConnector.CFG_JOB_CLEANUP_HOURS);

        // job quartz config
        var jobData = new HashMap<String, Object>();
        jobData.put(CleanupJob.SNOWFLAKE_CONNECTION, connection);
        jobData.put(CleanupJob.INGEST_TABLE_NAME, ingestTableName);
        jobData.put(CleanupJob.CLEANUP_HOURS, intervalHoursCleanup);

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
    }

    private List<String> extractPK(SinkRecord record) {
        if (pks.isEmpty()) {
            if (record.keySchema() == null || record.valueSchema() == null) {
                throw new RuntimeException("Schema is required for key and value in SinkRecord, using this profile");
            }
            for (Field field : record.keySchema().fields()) {
                pks.add(field.name());
            }
        }

        return pks;
    }


    private String convertPKToStringKey(SinkRecord record) {
        if (!(record.key() instanceof Struct keyStruct)) {
            logger.error("Expected key to be a Struct, but got: {}", record.key().getClass().getName());
            throw new RuntimeException("Expected key to be a Struct, but got: " + record.key().getClass().getName());
        }
        var pkValues = new ArrayList<String>();
        for (String pk : pks) {
            var value = keyStruct.get(pk);
            if (value == null) {
                logger.error("Value for field '{}' is null in record: {}", pk, record);
                throw new RuntimeException("Value for field '" + pk + "' is null");
            }
            pkValues.add(value.toString());
        }

        return String.join("+", pkValues);
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

    private String buildExcludeColumns() {
        return String.join(",", IHPARTITION, IHDATETIME, IHBLOCKID, IHOFFSET, IHOP, IHTOPIC);
    }

    private String buildPkWhereClause(List<String> pks) {
        return pks.stream()
                .map(col -> String.format("%s.%s = %s.%s", "final", col, "ingest", col))
                .reduce((a, b) -> a + " and " + b).orElseThrow();
    }

    private ByteArrayOutputStream prepareOrderedColumnsBasedOnTargetTable(String blockID, List<String> columnsFromTable) throws Throwable {

        var startTime = System.currentTimeMillis();
        var csvInMemory = new ByteArrayOutputStream();
        var stringBuilder = new StringBuilder();

        flushHasDeletedRecords = false;
        flushHasUpdatedRecords = false;
        flushHasInsertedRecords = false;

        boolean loggedDebugForFirstLine = false;
        for (var recordInBuffer : buffer.values()) {
            var count = 0;
            var op = recordInBuffer.op;

            if (debeziumOperation.d.toString().equalsIgnoreCase(op)) {
                flushHasDeletedRecords = true;
            }

            if (debeziumOperation.c.toString().equalsIgnoreCase(op)) {
                flushHasInsertedRecords = true;
            }

            if (debeziumOperation.u.toString().equalsIgnoreCase(op)) {
                flushHasUpdatedRecords = true;
            }

            for (String columnFromSnowflakeTable : columnsFromTable) {

                if (columnFromSnowflakeTable.equalsIgnoreCase(IHBLOCKID)) {
                    var strBuffer = "\"" + blockID + "\"";
                    stringBuilder.append(strBuffer);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHOP)) {
                    var strBuffer = "\"" + recordInBuffer.op + "\"";
                    stringBuilder.append(strBuffer);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHTOPIC)) {
                    var strBuffer = "\"" + recordInBuffer.topic + "\"";
                    stringBuilder.append(strBuffer);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHDATETIME)) {
                    var strBuffer = "\"" + recordInBuffer.timestamp + "\"";
                    stringBuilder.append(strBuffer);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHPARTITION)) {
                    var strBuffer = "\"" + recordInBuffer.partition + "\"";
                    stringBuilder.append(strBuffer);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHOFFSET)) {
                    var strBuffer = "\"" + recordInBuffer.offset + "\"";
                    stringBuilder.append(strBuffer);
                } else {
                    Object valueFromRecord = recordInBuffer.event.get(columnFromSnowflakeTable);
                    if (valueFromRecord != null) {
                        if (containsAny(columnFromSnowflakeTable, timeFieldsConvert)) {
                            var valueFromRecordAsLong = (long) valueFromRecord;
                            valueFromRecord = LocalDateTime.ofInstant(Instant.ofEpochMilli(valueFromRecordAsLong),
                                    TimeZone.getDefault().toZoneId()).toString();
                        } else if (containsAny(columnFromSnowflakeTable, dateFieldsConvert)) {
                            var valueFromRecordAsLong = (long) valueFromRecord;
                            var daysInSeconds = valueFromRecordAsLong * 24 * 60 * 60;
                            valueFromRecord = LocalDate.ofInstant(Instant.ofEpochSecond(daysInSeconds),
                                    TimeZone.getDefault().toZoneId()).toString();
                        } else if (containsAny(columnFromSnowflakeTable, timeFieldsConvert)) {
                            var valueFromRecordAsLong = (long) valueFromRecord;
                            valueFromRecord = LocalTime.ofNanoOfDay(valueFromRecordAsLong).toString();
                        }

                        valueFromRecord = valueFromRecord.toString().replaceAll("\"", "\"\"");
                        var strBuffer = "\"" + valueFromRecord + "\"";
                        stringBuilder.append(strBuffer);

                    } else {
                        logger.warn("Column {} not found on buffer, inserted empty value", columnFromSnowflakeTable);
                    }
                }

                if (count < columnsFromTable.size() - 1) {
                    stringBuilder.append(",");
                }

                count++;
            }


            stringBuilder.append("\n");
            if (!loggedDebugForFirstLine && logger.isDebugEnabled()) {
                logger.debug("First lines of csv: {}", stringBuilder);
                loggedDebugForFirstLine = true;
            }
        }

        if (!stringBuilder.isEmpty()) {
            csvInMemory.write(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
        }
        var endTime = System.currentTimeMillis();
        logger.debug("Prepared csv in memory in {} ms", endTime - startTime);
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
