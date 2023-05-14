package uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentSerializer implements Serializer<DocumentRow> {
    /*
    Serialize each DocumentRow object as json for publishing to Kafka
     */
    private static final Logger log = LoggerFactory.getLogger(
        DocumentSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, DocumentRow document) {
        /*
        Although the topic argument is not used, it is required by the Kafka
        classes which we pass this DocumentSerializer to.
         */
        try {
            if (document == null){
                log.warn("Null data received by DocumentSerializer");
                return null;
            }
            return objectMapper.writeValueAsBytes(document);
        } catch (Exception e) {
            log.error(e.toString());
            throw new SerializationException("Error during serialization");
        }
    }
}
