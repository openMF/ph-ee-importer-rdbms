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
public class TransferTransformerConfig {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @PostConstruct
    public void setup() {
        logger.info("Loaded Transfer transformers for {} flows from configuration", flows.size());
    }

    private List<Flow> flows;

    public void setFlows(List<Flow> flows) {
        this.flows = flows;
    }

    public List<Flow> getFlows() {
        return flows;
    }

    @Data
    public static class Flow {
        private String name;
        private String direction;

        public void setName(String name) {
            this.name = name;
        }

        public void setDirection(String direction) {
            this.direction = direction;
        }

        public void setTransformers(List<Transformer> transformers) {
            this.transformers = transformers;
        }

        public String getName() {
            return name;
        }

        public String getDirection() {
            return direction;
        }

        public List<Transformer> getTransformers() {
            return transformers;
        }

        private List<Transformer> transformers;

    }

    @Data
    public static class Transformer {
        private String field;

        public void setField(String field) {
            this.field = field;
        }

        public void setVariableName(String variableName) {
            this.variableName = variableName;
        }

        public void setJsonPath(String jsonPath) {
            this.jsonPath = jsonPath;
        }

        public void setConstant(String constant) {
            this.constant = constant;
        }

        public void setXpath(String xpath) {
            this.xpath = xpath;
        }

        private String variableName;
        private String jsonPath;

        public String getField() {
            return field;
        }

        public String getVariableName() {
            return variableName;
        }

        public String getJsonPath() {
            return jsonPath;
        }

        public String getConstant() {
            return constant;
        }

        public String getXpath() {
            return xpath;
        }

        private String constant;
        private String xpath;
    }

    public Optional<Flow> findFlow(String name) {
        return flows.stream().filter(flow -> name.equalsIgnoreCase(flow.getName())).findAny();
    }
}
