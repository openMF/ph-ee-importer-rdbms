package hu.dpc.phee.operator.config;

import hu.dpc.phee.operator.config.transformer.Flow;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Optional;

@EnableConfigurationProperties
@Configuration
@ConfigurationProperties(prefix = "transfer")
@Data
public class TransferTransformerConfig {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @PostConstruct
    public void setup() {
        if (flows == null) {
            throw new RuntimeException("Invalid configuration, no Transfer transformers configured");
        }
        logger.info("Loaded Transfer transformers for {} flows from configuration", flows.size());
    }

    private List<Flow> flows;

    public Optional<Flow> findFlow(String name) {
        return flows.stream().filter(flow -> name.equalsIgnoreCase(flow.getName())).findAny();
    }
}