package hu.dpc.phee.operator.streams.impl.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "importer")
public record ImporterConfig(Kafka kafka) {

    public record Kafka(String topic, int aggregationWindowSeconds, int aggregationAfterEndSeconds) {
    }
}