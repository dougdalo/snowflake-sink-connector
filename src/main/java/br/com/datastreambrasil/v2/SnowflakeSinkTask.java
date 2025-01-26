package br.com.datastreambrasil.v2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
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
    private final List<String> pks = new ArrayList<>();
    private final List<String> timestampFieldsConvertToSeconds = new ArrayList<>();
    private final Collection<Map<String, Object>> buffer = new ArrayList<>();
    private final List<String> columnsFromSnowflake = new ArrayList<>();
    private static final String PAYLOAD = "payload";
    private static final String AFTER = "after";
    private static final String BEFORE = "before";
    private static final String OP = "op";
    private static final String IHTOPIC = "ih_topic";
    private static final String IHOFFSET = "ih_offset";
    private static final String IHPARTITION = "ih_partition";
    private static final String IHOP = "ih_op";
    private static final String DELETE = "d";

    @Override
    public String version() {
        return SnowflakeSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        try {
            //init configs
            stageName = map.get(SnowflakeSinkConnector.CFG_STAGE_NAME);
            tableName = map.get(SnowflakeSinkConnector.CFG_TABLE_NAME);
            schemaName = map.get(SnowflakeSinkConnector.CFG_SCHEMA_NAME);

            if (map.containsKey(SnowflakeSinkConnector.CFG_PK_LIST)) {
                pks.addAll(Arrays.stream(map.get(SnowflakeSinkConnector.CFG_PK_LIST).split(",")).toList());
            }

            if (map.containsKey(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT_SECONDS)) {
                timestampFieldsConvertToSeconds.addAll(Arrays.stream(map.get(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT_SECONDS).split(",")).toList());
            }

            //init connection
            var properties = new Properties();
            properties.put("user", map.get(SnowflakeSinkConnector.CFG_USER));
            properties.put("password", map.get(SnowflakeSinkConnector.CFG_PASSWORD));
            connection = DriverManager.getConnection(map.get(SnowflakeSinkConnector.CFG_URL), properties);
            snowflakeConnection = connection.unwrap(SnowflakeConnection.class);
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
                    LOGGER.error("Null values for topic or kafkaPartition. Topic {}, KafkaPartition {}", record.topic(), record.kafkaPartition());
                    throw new RuntimeException("Null values for topic or kafkaPartition");
                }

                //topic,partition,offset,operation
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
            LOGGER.debug("Preparing to send {} records from buffer. To stage {} and table {}", buffer.size(), stageName, tableName);


            try (var csvToInsert = prepareOrderedColumnsBasedOnTargetTable();
                 var inputStream = new ByteArrayInputStream(csvToInsert.toByteArray())){
                snowflakeConnection.uploadStream(stageName, "/", inputStream,
                        destFileName, true);
                try (var stmt = connection.createStatement()) {
                    String command1 = String.format("""
                            CREATE OR REPLACE TEMPORARY TABLE "%s" AS
                            SELECT %s FROM @%s/%s.gz;""",destFileName,generateSelectFieldsInStageToMerge(), stageName,destFileName);
                    String command2 = String.format("""
                                MERGE INTO %s target USING (select * from "%s") source ON %s
                                WHEN MATCHED AND source.ih_op = 'd' THEN DELETE
                                WHEN MATCHED AND source.ih_op <> 'd' THEN
                                  %s
                                WHEN NOT MATCHED THEN
                                  %s;""", tableName, destFileName,
                                generateJoinClauseToMerge(),
                                generateUpdateSetClauseToMerge(),generateInsertClauseToMerge());

                    LOGGER.debug("statement 1: {}", command1);
                    LOGGER.debug("statement 2: {}", command2);
                    stmt.executeUpdate(command1);
                    stmt.executeUpdate(command2);
                }
            }


        } catch (Throwable e) {
            LOGGER.error("Error while flushing Snowflake connector", e);
            throw new RuntimeException("Error while flushing", e);
        } finally {
            buffer.clear();

            try {
                try (var stmt = connection.createStatement()){
                    String removeFileFromStage = String.format("REMOVE @%s/%s.gz", stageName, destFileName);
                    stmt.execute(removeFileFromStage);
                }
            }catch (Throwable e2){
                LOGGER.error("Error while removing file [{}] from stage {}", destFileName, stageName, e2);
            }
        }
    }

    private String generateSelectFieldsInStageToMerge() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columnsFromSnowflake.size(); i++){
            sb.append(String.format(" $%d %s,", i+1, columnsFromSnowflake.get(i)));
        }

        sb.append(String.format("$%d %s", columnsFromSnowflake.size()+1, IHOP));
        return sb.toString();
    }

    private String generateInsertClauseToMerge() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT (");
        for (int i = 0; i < columnsFromSnowflake.size(); i++){
            sb.append(String.format("%s", columnsFromSnowflake.get(i)));

            if (i+1 < columnsFromSnowflake.size()) {
                sb.append(", ");
            }
        }

        sb.append(") VALUES (");
        for (int i = 0; i < columnsFromSnowflake.size(); i++){
            sb.append(String.format("source.%s", columnsFromSnowflake.get(i)));

            if (i+1 < columnsFromSnowflake.size()) {
                sb.append(", ");
            }
        }
        sb.append(")");

        return sb.toString();
    }

    private String generateUpdateSetClauseToMerge() {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE SET ");
        for (int i = 0; i < columnsFromSnowflake.size(); i++){
            sb.append(String.format("target.%s = source.%s", columnsFromSnowflake.get(i),columnsFromSnowflake.get(i)));

            if (i+1 < columnsFromSnowflake.size()) {
                sb.append(", ");
            }
        }

        return sb.toString();
    }

    private String generateJoinClauseToMerge() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pks.size(); i++){
            sb.append(String.format(" target.%s = source.%s", pks.get(i),pks.get(i)));

            if (i+1 < pks.size()){
                sb.append(", ");
            }
        }

        return sb.toString();
    }


    @Override
    public void stop() {
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
        if (columnsFromTable.isEmpty()){
            throw new RuntimeException("Empty columns returned from target table "+tableName+", schema "+ schemaName);
        }

        LOGGER.debug("Columns mapped from target table: {}", String.join(",", columnsFromTable));
        columnsFromSnowflake.clear();
        columnsFromSnowflake.addAll(columnsFromTable);

        var csvInMemory = new ByteArrayOutputStream();

        for (var recordInBuffer : buffer) {
            for (String columnFromSnowflakeTable : columnsFromTable) {
                if (recordInBuffer.containsKey(columnFromSnowflakeTable)) {
                    var valueFromRecord = recordInBuffer.get(columnFromSnowflakeTable);
                    if (containsAny(columnFromSnowflakeTable, timestampFieldsConvertToSeconds)) {
                        var valueFromRecordAsLong = (long) valueFromRecord;
                        valueFromRecord = valueFromRecordAsLong / 1000;
                    }

                    if (valueFromRecord != null) {
                        var strBuffer = "\"" + valueFromRecord + "\"";
                        csvInMemory.writeBytes(strBuffer.getBytes());
                    }
                } else {
                    LOGGER.warn("Column {} not found on buffer, inserted empty value", columnFromSnowflakeTable);
                }

                csvInMemory.writeBytes(",".getBytes());
            }

            var strBuffer = "\"" +recordInBuffer.get(IHOP).toString() + "\"";
            csvInMemory.writeBytes(strBuffer.getBytes());
            csvInMemory.writeBytes("\n".getBytes());
        }

        return csvInMemory;
    }


    private boolean containsAny(String checkValue, List<String> values){
        for (String s : values){
            if (s.trim().equalsIgnoreCase(checkValue.trim())){
                return true;
            }
        }

        return false;
    }
}