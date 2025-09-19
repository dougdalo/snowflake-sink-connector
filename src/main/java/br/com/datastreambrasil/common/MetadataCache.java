package br.com.datastreambrasil.common;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple in-memory cache used to avoid repeating expensive metadata lookups in Snowflake.
 */
public final class MetadataCache {

    private MetadataCache() {
    }

    private static final ConcurrentMap<MetadataKey, List<String>> CACHE = new ConcurrentHashMap<>();

    @FunctionalInterface
    public interface SqlSupplier<T> {
        T get() throws SQLException;
    }

    public static List<String> getOrLoad(String schemaName, String tableName, SqlSupplier<List<String>> loader)
        throws SQLException {
        Objects.requireNonNull(schemaName, "schemaName");
        Objects.requireNonNull(tableName, "tableName");

        var key = new MetadataKey(schemaName, tableName);
        try {
            return CACHE.computeIfAbsent(key, ignored -> {
                try {
                    var loaded = loader.get();
                    if (loaded == null || loaded.isEmpty()) {
                        throw new IllegalStateException(
                            "Empty columns returned from target table " + tableName + ", schema " + schemaName);
                    }
                    return Collections.unmodifiableList(new ArrayList<>(loaded));
                } catch (SQLException e) {
                    throw new MetadataLoadException(e);
                }
            });
        } catch (MetadataLoadException e) {
            throw e.getCause();
        }
    }

    public static void clear() {
        CACHE.clear();
    }

    private record MetadataKey(String schemaName, String tableName) {
        private MetadataKey {
            schemaName = schemaName.toUpperCase(Locale.ROOT);
            tableName = tableName.toUpperCase(Locale.ROOT);
        }
    }

    private static final class MetadataLoadException extends RuntimeException {
        private final SQLException cause;

        private MetadataLoadException(SQLException cause) {
            super(cause);
            this.cause = cause;
        }

        @Override
        public SQLException getCause() {
            return cause;
        }
    }
}
