package uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DocumentSerializerTest {
    /*
    Test serialization of documents
     */

    @Test
    void nullSerializationTest() {
        /*
        Test that the Serializer returns null if given a null document
         */
        DocumentSerializer serializer = new DocumentSerializer();
        byte[] actual = serializer.serialize("someTopic", null);
        assertEquals(null, actual);
    }

    @Test
    void exampleSerializationTest() throws Exception {
        // given a document
        DocumentRow doc = new DocumentRow();
        String documentID = "id1";
        Timestamp timestamp = Timestamp.valueOf("2007-09-23 10:10:10.0");
        String relevantDocument = "Y";
        String s3Key = "s3://some-bucket/some-file.pdf";
        doc.setDocument_id(documentID);
        doc.setUploaded_date(timestamp);
        doc.setRelevant_document(relevantDocument);
        doc.setS3_key(s3Key);


        // serialize it
        DocumentSerializer serializer = new DocumentSerializer();
        byte[] bytes = serializer.serialize("someTopic", doc);

        // read the bytes
        Map<String, String> document = null;
        try {
            document = new ObjectMapper().readValue(bytes, HashMap.class);
        }
        catch (IOException e) {
            throw e;
        }
        long epochTime = 1190538610000L;
        assertEquals(document.get("uploaded_date"), epochTime);
        assertEquals(document.get("relevant_document"), "Y");
        assertEquals(document.get("s3_key"), "s3://some-bucket/some-file.pdf");
        assertEquals(document.get("document_id"), "id1");
    }
}
