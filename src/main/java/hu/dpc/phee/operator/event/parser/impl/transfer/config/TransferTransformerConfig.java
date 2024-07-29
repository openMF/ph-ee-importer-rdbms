package hu.dpc.phee.operator.event.parser.impl.transfer.config;

import hu.dpc.phee.operator.config.model.Flow;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@EnableConfigurationProperties
@Configuration
@ConfigurationProperties(prefix = "transfer")
@Data
public class TransferTransformerConfig {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final List<Flow> flows = new ArrayList<>();

    @PostConstruct
    public void setup() {
        logger.info("Loaded Transfer transformers for {} flows from configuration", flows.size());
    }

    public Optional<Flow> findFlow(String name) {
        return flows.stream().filter(flow -> name.equalsIgnoreCase(flow.getName())).findAny();
    }
}