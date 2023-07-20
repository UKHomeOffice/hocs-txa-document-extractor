package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentRow;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {
    /*
    Configuration for the Kafka Producer used in the Spring Batch job.
     */
    private @Value("${kafka.bootstrap_servers}") String bootstrapServers;
    private @Value("${kafka.ingest_topic}") String ingestTopic;
    private @Value("${kafka.delete_topic}") String deleteTopic;
    private @Value("${kafka.use_ssl}") boolean useSSL;
    private @Value("${mode.delete}") boolean deletes;

    @Bean
    public ProducerFactory<String, DocumentRow> producerFactory() {
        /*
        Configuration for the Kafka Producer.
        See https://kafka.apache.org/documentation/#producerconfigs for property definitions
         */
        Map<String, Object> configProperties = new HashMap<>();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DocumentSerializer.class);

        // Use TLS for the real kafka clusters but not for a local docker-composed kafka cluster
        if (useSSL) {
            configProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
            configProperties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
            configProperties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/etc/msk-certs/tls.key");
        }

        return new DefaultKafkaProducerFactory<>(configProperties);
    }

    @Bean
    public KafkaTemplate<String, DocumentRow> kafkaTemplate() {
        /*
        Sets the topic the producer should send messages to.
         */
        KafkaTemplate<String, DocumentRow> kafkaTemplate = new KafkaTemplate<String, DocumentRow>(producerFactory());
        if (deletes) {
            kafkaTemplate.setDefaultTopic(deleteTopic);
        } else {
            kafkaTemplate.setDefaultTopic(ingestTopic);
        }
        return kafkaTemplate;
    }
}
