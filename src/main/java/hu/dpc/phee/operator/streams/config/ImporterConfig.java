package hu.dpc.phee.operator.streams.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@EnableConfigurationProperties
@Configuration
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