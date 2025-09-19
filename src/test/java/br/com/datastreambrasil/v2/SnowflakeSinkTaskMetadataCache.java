package br.com.datastreambrasil.v2;

import br.com.datastreambrasil.common.MetadataCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SnowflakeSinkTaskMetadataCache {

    @BeforeEach
    void resetCache() {
        MetadataCache.clear();
    }

    @Test
    void shouldReuseMetadataFromCache() throws Exception {
        var connectionA = mock(Connection.class);
        var metadataA = mock(DatabaseMetaData.class);
        var resultSet = mock(ResultSet.class);

        when(connectionA.getMetaData()).thenReturn(metadataA);
        when(metadataA.getColumns(null, "PUBLIC", "CUSTOMER", null)).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("COLUMN_NAME")).thenReturn("ID", "UPDATED_AT");

        var taskA = new SnowflakeSinkTask();
        setField(taskA, "connection", connectionA);
        setField(taskA, "schemaName", "public");
        addIgnoreColumn(taskA, "UPDATED_AT");

        var columns = taskA.resolveColumnsFromMetadata("customer");
        assertEquals(List.of("ID"), columns);

        verify(metadataA, times(1)).getColumns(null, "PUBLIC", "CUSTOMER", null);

        var connectionB = mock(Connection.class, invocation -> {
            throw new AssertionError("Metadata should not be fetched again");
        });

        var taskB = new SnowflakeSinkTask();
        setField(taskB, "connection", connectionB);
        setField(taskB, "schemaName", "public");
        addIgnoreColumn(taskB, "UPDATED_AT");

        var cachedColumns = taskB.resolveColumnsFromMetadata("customer");
        assertEquals(List.of("ID"), cachedColumns);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = SnowflakeSinkTask.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void addIgnoreColumn(SnowflakeSinkTask task, String value) throws Exception {
        Field field = SnowflakeSinkTask.class.getDeclaredField("ignoreColumns");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        var list = (List<String>) field.get(task);
        list.add(value);
    }
}
