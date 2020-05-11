package hu.dpc.phee.operator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.security.oauth2.provider.token.store.JwtAccessTokenConverter;

@EnableScheduling
@SpringBootApplication
public class KafkaOperationsApplication {

    static {
        // NOTE zeebe timestamps are also GMT, parsed dates in DB should also use GMT to match this
        System.setProperty("user.timezone", "GMT");
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaOperationsApplication.class, args);
    }

    /**
     * Dummy bean to skip public key fetch from authorization server
     */
    @Bean
    @ConditionalOnExpression("${rest.authorization.enabled:false} == false")
    public JwtAccessTokenConverter jwtTokenEnhancer() {
        return new JwtAccessTokenConverter();
    }
}
