package hu.dpc.phee.operator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaOperatorApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaOperatorApplication.class, args);
	}

}
