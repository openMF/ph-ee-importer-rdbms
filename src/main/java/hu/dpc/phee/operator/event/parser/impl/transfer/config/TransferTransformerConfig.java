package hu.dpc.phee.operator.event.parser.impl.transfer.config;

import hu.dpc.phee.operator.config.model.Flow;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class TransferTransformerConfig {

    private final List<Flow> flows = new ArrayList<>();

    @PostConstruct
    public void setup() {
        log.info("Loaded Transfer transformers for {} flows from configuration", flows.size());
    }

    public Optional<Flow> findFlow(String name) {
        return flows.stream().filter(flow -> name.equalsIgnoreCase(flow.getName())).findAny();
    }
}