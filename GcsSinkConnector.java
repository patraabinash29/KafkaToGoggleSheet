package io.aiven.kafka.connect.gcs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GcsSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(GcsSinkConnector.class);

    private Map<String, String> configProps;
    private GcsSinkConfig config;

    // required by Connect
    public GcsSinkConnector() {
    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props, "props cannot be null");

        this.configProps = Collections.unmodifiableMap(props);
        this.config = new GcsSinkConfig(props);
        log.info("Starting connector {}", config.getConnectorName());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GcsSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            final Map<String, String> config = new HashMap<>(configProps);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do.
        log.info("Stopping connector {}", config.getConnectorName());
    }

    @Override
    public ConfigDef config() {
        return GcsSinkConfig.configDef();
    }
}
