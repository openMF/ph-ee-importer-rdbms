package hu.dpc.phee.operator.event.parser.impl.card.clearing.config;

import hu.dpc.phee.operator.config.model.Flow;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@ConfigurationProperties(prefix = "card-transaction")
@Data
public class CardTransactionTransformerConfig {

    private final List<Flow> flows = new ArrayList<>();

    public Optional<Flow> findFlow(String name) {
        return flows.stream().filter(flow -> name.equalsIgnoreCase(flow.getName())).findAny();
    }
}