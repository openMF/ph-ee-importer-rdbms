package hu.dpc.phee.operator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaOperatorApplication {

	static {
		// NOTE zeebe timestamps are also GMT, parsed dates in DB should also use GMT to match this
		System.setProperty("user.timezone", "GMT");
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaOperatorApplication.class, args);
	}

}
