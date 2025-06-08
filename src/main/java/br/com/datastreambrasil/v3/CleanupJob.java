package br.com.datastreambrasil.v3;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class CleanupJob implements Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(CleanupJob.class);

    protected static final String INGEST_TABLE_NAME = "ingest_table_name";
    protected static final String CLEANUP_HOURS = "cleanup_hours";
    protected static final String SNOWFLAKE_CONNECTION = "snowflake_connection";

    @Override
    public void execute(JobExecutionContext context) {
        var jobData = context.getMergedJobDataMap();
        var ingest = (String) jobData.get(INGEST_TABLE_NAME);
        var connection = (Connection) jobData.get(SNOWFLAKE_CONNECTION);
        var intervalHours = jobData.get(CLEANUP_HOURS);

        var deleteQuery = String.format("""
                        delete from %s ingest where ih_datetime + interval '%s hour' < sysdate()
                """, ingest, intervalHours);

        LOGGER.debug("Executing delete query: {}", deleteQuery);
        try (var stmt = connection.createStatement()) {
            stmt.executeUpdate(deleteQuery);
        } catch (SQLException e) {
            LOGGER.error("Error while executing delete query", e);
        }

    }

}
