package hu.dpc.phee.operator.streams.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "importer")
@Data
public class ImporterConfig {

    private Kafka kafka;

    @Data
    public static class Kafka {
        String topic;
        int aggregationWindowSeconds;
        int aggregationAfterEndSeconds;
    }
}