package hu.dpc.phee.operator.config;

import com.baasflow.commons.events.internal.KafkaHealthIndicator;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.streams.StreamsConfig.*;

@EnableKafka
@EnableKafkaStreams
@Configuration
public class KafkaConfiguration {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${kafka.brokers}")
    private String kafkaBrokers;

    @Value("${kafka.consumer-group}")
    private String consumerGroup;

    @Value("${kafka.msk}")
    private boolean msk;

    @Value("${kafka.commit-interval-ms}")
    private int commitIntervalMs;

    @Value("${kafka.listener.concurrency}")
    private int listenerConcurrency;

    @Value("${kafka.listener.poll-timeout-ms}")
    private int listenerPollTimeoutMs;

    @Autowired
    KafkaHealthIndicator kafkaHealthIndicator;

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(listenerConcurrency);
        factory.getContainerProperties().setPollTimeout(listenerPollTimeoutMs);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        String hostname = buildKafkaClientId(logger);
        logger.info("configuring Kafka consumer '{}' for bootstrap servers: {}", hostname, kafkaBrokers);

        Map<String, Object> properties = commonProperties();
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, hostname);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, hostname);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return new DefaultKafkaConsumerFactory<>(properties);
    }

    private Map<String, Object> commonProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);

        if (msk) {
            logger.info("configuring Kafka client with AWS MSK support");
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            properties.put(SaslConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, "software.amazon.msk.auth.iam.IAMLoginModule required;");
            properties.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        }

        return properties;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig() {
        logger.info("configuring Kafka streams for consumer group '{}' for bootstrap servers: {}", consumerGroup, kafkaBrokers);

        Map<String, Object> properties = commonProperties();
        properties.put(APPLICATION_ID_CONFIG, consumerGroup);
        properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.put(COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        return new KafkaStreamsConfiguration(properties);
    }

    @Bean
    public StreamsBuilder streamsBuilder(StreamsBuilderFactoryBean factory) throws Exception {
        factory.setStateListener((newState, oldState) -> {
            logger.warn("Kafka streams state changed from {} to {}", oldState, newState);
            if (newState == KafkaStreams.State.RUNNING) {
                kafkaHealthIndicator.setHealthy();
            } else {
                kafkaHealthIndicator.setUnhealthy("Kafka streams state changed from " + oldState + " to " + newState);
            }
        });
        return factory.getObject();
    }

    private static String buildKafkaClientId(Logger logger) {
        String clientId;
        try {
            clientId = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.error("failed to resolve local hostname, picking random clientId");
            clientId = UUID.randomUUID().toString();
        }
        logger.info("using Kafka client id {}", clientId);
        return clientId;
    }
}
