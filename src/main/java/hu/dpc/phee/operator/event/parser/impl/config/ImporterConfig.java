package hu.dpc.phee.operator.event.parser.impl.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties
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