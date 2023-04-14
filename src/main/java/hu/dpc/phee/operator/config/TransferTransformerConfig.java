package hu.dpc.phee.operator.config;

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
        logger.info("Loaded Transfer transformers for {} flows from configuration", flows.size());
    }

    private List<Flow> flows;

    @Data
    public static class Flow {
        private String name;
        private String direction;
        private List<Transformer> transformers;

    }

    @Data
    public static class Transformer {
        private String field;
        private String variableName;
        private String jsonPath;
        private String constant;
        private String xpath;
    }

    public Optional<Flow> findFlow(String name) {
        return flows.stream().filter(flow -> name.equalsIgnoreCase(flow.getName())).findAny();
    }
}
