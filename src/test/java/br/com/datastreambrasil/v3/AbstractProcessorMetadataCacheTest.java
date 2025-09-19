package br.com.datastreambrasil.v3;

import br.com.datastreambrasil.common.MetadataCache;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractProcessorMetadataCacheTest {

    @BeforeEach
    void setUp() {
        MetadataCache.clear();
    }

    @Test
    void shouldReuseCachedMetadataAcrossProcessors() throws Exception {
        var firstConnection = mock(Connection.class);
        var firstMetadata = mock(DatabaseMetaData.class);
        var finalResultSet = mock(ResultSet.class);
        var ingestResultSet = mock(ResultSet.class);

        when(firstConnection.getMetaData()).thenReturn(firstMetadata);
        when(firstMetadata.getColumns(null, "PUBLIC", "CUSTOMER", null)).thenReturn(finalResultSet);
        when(firstMetadata.getColumns(null, "PUBLIC", "CUSTOMER_INGEST", null)).thenReturn(ingestResultSet);

        when(finalResultSet.next()).thenReturn(true, true, true, false);
        when(finalResultSet.getString("COLUMN_NAME")).thenReturn("ID", "UPDATED_AT", "ID");
        when(ingestResultSet.next()).thenReturn(true, false);
        when(ingestResultSet.getString("COLUMN_NAME")).thenReturn("ID");

        var processorA = new TestProcessor();
        processorA.connection = firstConnection;
        processorA.schemaName = "public";
        processorA.tableName = "customer";
        processorA.ingestTableName = "customer_ingest";
        processorA.ignoreColumns.add("UPDATED_AT");

        processorA.configMetadata();

        assertIterableEquals(List.of("ID"), processorA.columnsFinalTable);
        assertIterableEquals(List.of("ID"), processorA.columnsIngestTable);

        verify(firstMetadata, times(1)).getColumns(null, "PUBLIC", "CUSTOMER", null);
        verify(firstMetadata, times(1)).getColumns(null, "PUBLIC", "CUSTOMER_INGEST", null);

        var secondConnection = mock(Connection.class, invocation -> {
            throw new AssertionError("Metadata should not be fetched again");
        });

        var processorB = new TestProcessor();
        processorB.connection = secondConnection;
        processorB.schemaName = "public";
        processorB.tableName = "customer";
        processorB.ingestTableName = "customer_ingest";
        processorB.ignoreColumns.add("UPDATED_AT");

        processorB.configMetadata();

        assertEquals(List.of("ID"), processorB.columnsFinalTable);
        assertEquals(List.of("ID"), processorB.columnsIngestTable);
    }

    private static final class TestProcessor extends AbstractProcessor {

        @Override
        protected void extraConfigsOnStart(AbstractConfig config) {
        }

        @Override
        protected void put(Collection<SinkRecord> collection) {
        }

        @Override
        protected void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        }

        @Override
        protected void stop() {
        }
    }
}
