package uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

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
        String uuid = "00000000-aaaa-bbbb-cccc-000000000000";
        String externalReferenceUuid = "00000000-aaaa-bbbb-cccc-0000000000a1";
        String caseType = "a1";
        String type = "ORIGINAL";
        String pdfLink = "some-file.pdf";
        String status = "UPLOADED";
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        Timestamp updatedOn = Timestamp.valueOf("2007-09-23 10:10:10.0");
        Timestamp deletedOn = Timestamp.valueOf("2008-09-23 10:10:10.0");
        doc.setUuid(uuid);
        doc.setExternalReferenceUuid(externalReferenceUuid);
        doc.setCaseType(caseType);
        doc.setType(type);
        doc.setPdfLink(pdfLink);
        doc.setStatus(status);
        doc.setUpdatedOn(updatedOn);
        doc.setDeletedOn(deletedOn);

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
        long updatedOnEpoch = 1190542210000L;
        long deletedOnEpoch = 1222164610000L;
        assertEquals(uuid, document.get("uuid"));
        assertEquals(externalReferenceUuid, document.get("externalReferenceUuid"));
        assertEquals(caseType, document.get("caseType"));
        assertEquals(type, document.get("type"));
        assertEquals(pdfLink, document.get("pdfLink"));
        assertEquals(status, document.get("status"));
        assertEquals(updatedOnEpoch, document.get("updatedOn"));
        assertEquals(deletedOnEpoch, document.get("deletedOn"));
    }
}
