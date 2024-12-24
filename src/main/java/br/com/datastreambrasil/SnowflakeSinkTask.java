package br.com.datastreambrasil;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeSinkConnector.class);
    private Connection connection;
    private String stageName;
    private final StringWriter buffer = new StringWriter();
    private static final String PAYLOAD = "payload";
    private static final String AFTER = "after";
    private static final String BEFORE = "before";
    private static final String OP = "op";
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

            //init connection
            var properties = new Properties();
            properties.put("user", map.get(SnowflakeSinkConnector.CFG_USER));
            properties.put("password", map.get(SnowflakeSinkConnector.CFG_PASSWORD));
            connection = DriverManager.getConnection(map.get(SnowflakeSinkConnector.CFG_URL), properties);
        } catch (Throwable e) {
            LOGGER.error("Error while starting Snowflake connector", e);
            throw new RuntimeException("Error while starting Snowflake connector", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void put(Collection<SinkRecord> collection) {
        try {
            for (SinkRecord record : collection) {
                var mapValue = (Map<String, Object>) record.value();

                validateFieldOnMap(PAYLOAD, mapValue);
                var mapPayload = (Map<String, Object>) mapValue.get(PAYLOAD);

                validateFieldOnMap(OP, mapPayload);
                var op = mapPayload.get(OP);
                Map<String,Object> mapPayloadAfterBefore;
                if (op.equals(DELETE)) {
                    validateFieldOnMap(BEFORE, mapPayload);
                    mapPayloadAfterBefore = (Map<String, Object>) mapPayload.get(BEFORE);
                }else{
                    validateFieldOnMap(AFTER, mapPayload);
                    mapPayloadAfterBefore = (Map<String, Object>) mapPayload.get(AFTER);
                }

                for (Object value : mapPayloadAfterBefore.values()) {
                    buffer.append(String.valueOf(value)).append(",");
                }

                if (record.topic() == null || record.kafkaPartition() == null){
                    LOGGER.error("Null values for topic or kafkaPartition. Topic {}, KafkaPartition {}", record.topic(), record.kafkaPartition());
                    throw new RuntimeException("Null values for topic or kafkaPartition");
                }

                //topic,partition,offset,operation
                buffer.append(String.join(",", record.topic(), String.valueOf(record.kafkaOffset()),
                        record.kafkaPartition().toString(), mapPayload.get(OP).toString()));
                buffer.append("\n");
            }
        }catch (Throwable e){
            LOGGER.error("Error while putting records", e);
            throw new RuntimeException("Error while putting records", e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        try {
            if (buffer.toString().isEmpty()) {
                return;
            }

            var destFileName = UUID.randomUUID().toString();
            var inputStream = new ByteArrayInputStream(buffer.toString().getBytes());
            connection.unwrap(SnowflakeConnection.class).uploadStream(stageName, "/", inputStream,
                    destFileName, true);
            buffer.getBuffer().setLength(0);
        } catch (Throwable e) {
            LOGGER.error("Error while flushing Snowflake connector", e);
            throw new RuntimeException("Error while flushing", e);
        }
    }


    @Override
    public void stop() {
    }

    private void validateFieldOnMap(String fieldToValidate, Map<String,?> map){
        if (!map.containsKey(fieldToValidate) || map.get(fieldToValidate) == null) {
            LOGGER.error("Key [{}] is missing or null on json", fieldToValidate);
            throw new RuntimeException("missing or null key ["+fieldToValidate+"] on json");
        }
    }

}
