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
        String uuid = "00000000-aaaa-bbbb-cccc-000000000000";
        String externalReferenceUuid = "00000000-aaaa-bbbb-cccc-0000000000a1";
        String caseType = "a1";
        String type = "ORIGINAL";
        String pdfLink = "some-file.pdf";
        String status = "UPLOADED";
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
        long updatedOnEpoch = 1190538610000L;
        long deletedOnEpoch = 1222161010000L;
        assertEquals(document.get("uuid"), uuid);
        assertEquals(document.get("externalReferenceUuid"), externalReferenceUuid);
        assertEquals(document.get("caseType"), caseType);
        assertEquals(document.get("type"), type);
        assertEquals(document.get("pdfLink"), pdfLink);
        assertEquals(document.get("status"), status);
        assertEquals(document.get("updatedOn"), updatedOnEpoch);
        assertEquals(document.get("deletedOn"), deletedOnEpoch);
    }
}
