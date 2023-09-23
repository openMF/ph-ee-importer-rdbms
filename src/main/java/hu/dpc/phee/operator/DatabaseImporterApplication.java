package hu.dpc.phee.operator;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import hu.dpc.phee.operator.config.TransferTransformerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class})
@Configuration
@EnableConfigurationProperties(value = TransferTransformerConfig.class)
public class DatabaseImporterApplication {

    static {
        // NOTE zeebe timestamps are also GMT, parsed dates in DB should also use GMT to match this
        System.setProperty("user.timezone", "GMT");
    }

    public static void main(String[] args) {
        SpringApplication.run(DatabaseImporterApplication.class, args);
    }

    @Bean
    public CsvMapper csvMapper() {
        return new CsvMapper();
    }
}
