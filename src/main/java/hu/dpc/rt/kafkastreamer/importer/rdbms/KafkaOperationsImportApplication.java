package hu.dpc.rt.kafkastreamer.importer.rdbms;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaOperationsImportApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaOperationsImportApplication.class, args);
	}

}
