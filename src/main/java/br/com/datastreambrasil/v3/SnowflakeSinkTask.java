package br.com.datastreambrasil.v3;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class SnowflakeSinkTask extends SinkTask {

    private AbstractProcessor processor;

    @Override
    public String version() {
        return SnowflakeSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        AbstractConfig config = new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, map);

        switch (config.getString(SnowflakeSinkConnector.CFG_PROFILE)) {
            case "cdc_schema":
                processor = new CdcDbzSchemaProcessor();
                break;
            default:
                throw new RuntimeException("Unknown profile: " + config.getString(SnowflakeSinkConnector.CFG_PROFILE));
        }

        processor.start(config);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        processor.put(collection);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        processor.flush(currentOffsets);
    }

    @Override
    public void stop() {
        processor.stop();
    }
}