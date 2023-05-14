package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import org.apache.kafka.clients.producer.ProducerConfig;
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

     */
    private @Value("${kafka.bootstrap_servers}") String bootstrapServers;
    private @Value("${kafka.ingest_topic}") String ingestTopic;
    private @Value("${kafka.delete_topic}") String deleteTopic;

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

        return new DefaultKafkaProducerFactory<>(configProperties);
    }

    @Bean
    public KafkaTemplate<String, DocumentRow> kafkaTemplate() {
        KafkaTemplate<String, DocumentRow> kafkaTemplate = new KafkaTemplate<String, DocumentRow>(producerFactory());
        kafkaTemplate.setDefaultTopic(ingestTopic);
        return kafkaTemplate;
    }
}
