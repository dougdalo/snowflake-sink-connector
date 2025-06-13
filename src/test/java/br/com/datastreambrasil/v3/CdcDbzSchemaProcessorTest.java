package br.com.datastreambrasil.v3;

import br.com.datastreambrasil.v3.exception.InvalidStructException;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class CdcDbzSchemaProcessorTest {

    private static Schema valueAfterBeforeSchema;
    private static Schema valueSchema;
    private static Schema keySchema;

    @BeforeAll
    static void beforeTest() {
        valueAfterBeforeSchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("timestamp", Schema.INT64_SCHEMA)
                .field("time", Schema.INT64_SCHEMA)
                .field("date", Schema.INT64_SCHEMA)
                .field("desc", Schema.OPTIONAL_STRING_SCHEMA).build();
        valueSchema = SchemaBuilder.struct()
                .field("before", valueAfterBeforeSchema)
                .field("after", valueAfterBeforeSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();
        keySchema = SchemaBuilder.struct()
                .field("id", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Test
    void testPutSuccess() {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2025, 1, 20, 10, 30, 40);
        processor.put(generateCreateEvents(dt, "1", "2", "3"));
        processor.put(generateUpdateEvents(dt, "update 001", "10"));//this update should be ignored, because it will be overridden by the next update event
        processor.put(generateUpdateEvents(dt, "update 002", "10"));
        processor.put(generateDeleteEvents(dt, "20"));
        processor.put(generateDeleteEvents(dt, "3")); //this delete will override the create event for id 3
        assertEquals(5, processor.buffer.size());

        var itemIdx = "1";
        //assert item create
        assertEquals(itemIdx, processor.buffer.get(itemIdx).event().get("id"));
        assertEquals("Name " + itemIdx, processor.buffer.get(itemIdx).event().get("name"));
        assertEquals("c", processor.buffer.get(itemIdx).op());

        //assert item update
        itemIdx = "10";
        assertEquals(itemIdx, processor.buffer.get(itemIdx).event().get("id"));
        assertEquals("Name update 002 " + itemIdx, processor.buffer.get(itemIdx).event().get("name"));
        assertEquals("u", processor.buffer.get(itemIdx).op());

        //asert item delete
        itemIdx = "20";
        assertEquals(itemIdx, processor.buffer.get(itemIdx).event().get("id"));
        assertEquals("Name " + itemIdx, processor.buffer.get(itemIdx).event().get("name"));
        assertEquals("d", processor.buffer.get(itemIdx).op());

        itemIdx = "3";
        assertEquals(itemIdx, processor.buffer.get(itemIdx).event().get("id"));
        assertEquals("Name " + itemIdx, processor.buffer.get(itemIdx).event().get("name"));
        assertEquals("d", processor.buffer.get(itemIdx).op());
    }

    @Test
    void testPutFailWithInvalidValueSchema() {
        var processor = new CdcDbzSchemaProcessor();
        assertThrows(InvalidStructException.class, () -> processor.put(List.of(new SinkRecord(
                "test_topic",
                0,
                keySchema,
                new Struct(keySchema).put("id", "1"),
                valueSchema,
                "invalid_value", // This should be a Struct, but it's a String
                1
        ))));
    }

    @Test
    void testPutFailWithInvalidTopic() {
        var processor = new CdcDbzSchemaProcessor();
        assertThrows(InvalidStructException.class, () -> processor.put(List.of(new SinkRecord(
                null, // Invalid topic
                0,
                keySchema,
                new Struct(keySchema).put("id", "1"),
                valueSchema,
                "invalid_value", // This should be a Struct, but it's a String
                1
        ))));
    }

    @Test
    void testPutFailWithInvalidKeySchema() {
        var processor = new CdcDbzSchemaProcessor();
        assertThrows(InvalidStructException.class, () -> processor.put(List.of(new SinkRecord(
                "test_topic",
                0,
                keySchema,
                "invalid_key", // This should be a Struct, but it's a String
                valueSchema,
                new Struct(valueSchema)
                        .put("after", new Struct(valueAfterBeforeSchema)
                                .put("id", "1")
                                .put("name", "Name")
                                .put("created_at", LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()))
                        .put("op", CdcDbzSchemaProcessor.debeziumOperation.c.name()),
                1
        ))));
    }

    @Test
    void testPutFailWithFieldOpMissing() {
        var valueSchemaWithNoOpField = SchemaBuilder.struct()
                .field("before", valueAfterBeforeSchema)
                .field("after", valueAfterBeforeSchema)
                .build();
        var processor = new CdcDbzSchemaProcessor();
        assertThrows(InvalidStructException.class, () -> processor.put(List.of(new SinkRecord(
                "test_topic",
                0,
                keySchema,
                new Struct(keySchema).put("id", "1"),
                valueSchemaWithNoOpField,
                new Struct(valueSchema)
                        .put("after", new Struct(valueAfterBeforeSchema)
                                .put("id", "1")
                                .put("name", "Name")
                                .put("created_at", LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()))
                        .put("op", CdcDbzSchemaProcessor.debeziumOperation.c.name()),
                1
        ))));
    }

    @Test
    void testPutFailWithValueOpMissing() {
        var processor = new CdcDbzSchemaProcessor();
        assertThrows(InvalidStructException.class, () -> processor.put(List.of(new SinkRecord(
                "test_topic",
                0,
                keySchema,
                new Struct(keySchema).put("id", "1"),
                valueSchema,
                new Struct(valueSchema)
                        .put("after", new Struct(valueAfterBeforeSchema)
                                .put("id", "1")
                                .put("name", "Name")
                                .put("created_at", LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli())),
                1
        ))));
    }

    @Test
    void testPutFailWithNullKey() {
        var processor = new CdcDbzSchemaProcessor();
        assertThrows(InvalidStructException.class, () -> processor.put(List.of(new SinkRecord(
                "test_topic",
                0,
                keySchema,
                new Struct(keySchema).put("id", null), // Key should not be null
                valueSchema,
                new Struct(valueSchema)
                        .put("after", new Struct(valueAfterBeforeSchema)
                                .put("id", "1")
                                .put("name", "Name")
                                .put("created_at", LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()))
                        .put("op", CdcDbzSchemaProcessor.debeziumOperation.c.name()),
                1
        ))));
    }

    @Test
    void testFlushWithSuccess() throws SQLException {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.now();
        var statementMock = prepareToFlush(processor);
        processor.put(generateCreateEvents(dt, "1", "2", "3"));
        processor.flush(null);

        verify(statementMock, times(1)).executeUpdate(matches("INSERT.*"));
        verify(statementMock, times(0)).executeUpdate(matches("DELETE.*"));
        verify(statementMock, times(0)).executeUpdate(matches("UPDATE.*"));
        verify(statementMock, times(1)).executeUpdate(matches("COPY.*"));
        assertEquals(0, processor.buffer.size(), "Buffer should be empty after flush");
    }

    private Statement prepareToFlush(CdcDbzSchemaProcessor processor) throws SQLException {
        var statementMock = mockConnections(processor,
                List.of("id", "name", "timestamp", "time", "date", "desc"),
                List.of("id", "name", "timestamp", "time", "date", "desc", "ih_topic", "ih_offset", "ih_partition", "ih_op", "ih_datetime", "ih_blockid"),
                "test_table", "test_table_INGEST");
        processor.configParameters(generateConfig());
        processor.configMetadata();
        return statementMock;
    }

    private AbstractConfig generateConfig() {
        return new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, Map.of(
                "schema", "test_schema",
                "table", "test_table"
        ));
    }

    private Statement mockConnections(CdcDbzSchemaProcessor processor, List<String> columnsTable, List<String> columnsTableIngest, String table, String tableIngest) throws SQLException {
        processor.snowflakeConnection = mock(SnowflakeConnection.class);
        processor.connection = mock(Connection.class);
        var dbMetadataMock = mock(DatabaseMetaData.class);
        var resultSetMock = mock(ResultSet.class);
        var resultSetTableMock = mock(ResultSet.class);
        var resultSetTableIngestMock = mock(ResultSet.class);
        var statementMock = mock(java.sql.Statement.class);

        when(processor.connection.createStatement()).thenReturn(statementMock);
        when(statementMock.executeQuery(any())).thenReturn(resultSetMock);
        when(processor.connection.getCatalog()).thenReturn("test_catalog");
        when(processor.connection.getSchema()).thenReturn("test_schema");
        when(processor.connection.getMetaData()).thenReturn(dbMetadataMock);


        var columnsTableIterator = columnsTable.iterator();
        when(resultSetTableMock.next()).thenAnswer(a -> {
            if (columnsTableIterator.hasNext()) {
                return true;
            }
            return false;
        });
        when(resultSetTableMock.getString("COLUMN_NAME")).thenAnswer(a -> {
            if (columnsTableIterator.hasNext()) {
                return columnsTableIterator.next();
            }
            return null;
        });


        var columnsTableIngestIterator = columnsTableIngest.iterator();
        when(resultSetTableIngestMock.next()).thenAnswer(a -> {
            if (columnsTableIngestIterator.hasNext()) {
                return true;
            }
            return false;
        });
        when(resultSetTableIngestMock.getString("COLUMN_NAME")).thenAnswer(a -> {
            if (columnsTableIngestIterator.hasNext()) {
                return columnsTableIngestIterator.next();
            }
            return null;
        });

        when(dbMetadataMock.getColumns(any(), any(), eq(table.toUpperCase()), any())).thenReturn(resultSetTableMock);
        when(dbMetadataMock.getColumns(any(), any(), eq(tableIngest.toUpperCase()), any())).thenReturn(resultSetTableIngestMock);

        when(processor.connection.unwrap(SnowflakeConnection.class)).thenReturn(processor.snowflakeConnection);
        when(processor.connection.getMetaData()).thenReturn(dbMetadataMock);
        return statementMock;
    }

    private Collection<SinkRecord> generateCreateEvents(LocalDateTime dt, String... ids) {
        var records = new ArrayList<SinkRecord>();

        for (int i = 0; i < ids.length; i++) {
            var id = ids[i];
            records.add(new SinkRecord(
                    "test_topic",
                    0,
                    keySchema,
                    new Struct(keySchema).put("id", id),
                    valueSchema,
                    new Struct(valueSchema)
                            .put("after", new Struct(valueAfterBeforeSchema)
                                    .put("id", id)
                                    .put("name", "Name " + id)
                                    .put("timestamp", dt.toInstant(ZoneOffset.UTC).toEpochMilli())
                                    .put("time", dt.getLong(ChronoField.NANO_OF_DAY))
                                    .put("date", dt.getLong(ChronoField.EPOCH_DAY)))
                            .put("op", CdcDbzSchemaProcessor.debeziumOperation.c.name()),
                    i
            ));
        }

        return records;
    }

    private Collection<SinkRecord> generateUpdateEvents(LocalDateTime dt, String nameSuffix, String... ids) {
        var records = new ArrayList<SinkRecord>();

        for (int i = 0; i < ids.length; i++) {
            var createdAt = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli();
            var id = ids[i];
            records.add(new SinkRecord(
                    "test_topic",
                    0,
                    keySchema,
                    new Struct(keySchema).put("id", id),
                    valueSchema,
                    new Struct(valueSchema)
                            .put("before", new Struct(valueAfterBeforeSchema)
                                    .put("id", id)
                                    .put("name", "Name " + id)
                                    .put("timestamp", dt.toInstant(ZoneOffset.UTC).toEpochMilli())
                                    .put("time", dt.getLong(ChronoField.NANO_OF_DAY))
                                    .put("date", dt.getLong(ChronoField.EPOCH_DAY)))
                            .put("after", new Struct(valueAfterBeforeSchema)
                                    .put("id", id)
                                    .put("name", String.format("Name %s %s", nameSuffix, id))
                                    .put("timestamp", dt.toInstant(ZoneOffset.UTC).toEpochMilli())
                                    .put("time", dt.getLong(ChronoField.NANO_OF_DAY))
                                    .put("date", dt.getLong(ChronoField.EPOCH_DAY)))
                            .put("op", CdcDbzSchemaProcessor.debeziumOperation.u.name()),
                    i
            ));
        }

        return records;
    }

    private Collection<SinkRecord> generateDeleteEvents(LocalDateTime dt, String... ids) {
        var records = new ArrayList<SinkRecord>();

        for (int i = 0; i < ids.length; i++) {
            var createdAt = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli();
            var id = ids[i];
            records.add(new SinkRecord(
                    "test_topic",
                    0,
                    keySchema,
                    new Struct(keySchema).put("id", id),
                    valueSchema,
                    new Struct(valueSchema)
                            .put("before", new Struct(valueAfterBeforeSchema)
                                    .put("id", id)
                                    .put("name", "Name " + id)
                                    .put("timestamp", dt.toInstant(ZoneOffset.UTC).toEpochMilli())
                                    .put("time", dt.getLong(ChronoField.NANO_OF_DAY))
                                    .put("date", dt.getLong(ChronoField.EPOCH_DAY)))
                            .put("op", CdcDbzSchemaProcessor.debeziumOperation.d.name()),
                    i
            ));
        }

        return records;
    }

}