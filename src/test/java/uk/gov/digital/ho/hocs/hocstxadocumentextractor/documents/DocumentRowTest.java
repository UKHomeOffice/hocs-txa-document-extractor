package uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Timestamp;

public class DocumentRowTest {
    /*
    May not keep these unit tests of basic getters/setters - is it best practice to test this?
     */

    @Test
    void settersTest() {
        // given a document
        DocumentRow doc = new DocumentRow();

        // when values are set
        String documentID = "id1";
        Timestamp timestamp = Timestamp.valueOf("2007-09-23 10:10:10.0");
        String relevantDocument = "Y";
        String s3Key = "s3://some-bucket/some-file.pdf";
        doc.setDocument_id(documentID);
        doc.setUploaded_date(timestamp);
        doc.setRelevant_document(relevantDocument);
        doc.setS3_key(s3Key);

        // then they are set correctly
        assertEquals(doc.getDocument_id(), documentID);
        assertEquals(doc.getUploaded_date(), timestamp);
        assertEquals(doc.getRelevant_document(), relevantDocument);
        assertEquals(doc.getS3_key(), s3Key);
    }

    @Test
    void gettersTest() {
        // given a document
        DocumentRow doc = new DocumentRow();

        // when values are set
        String documentID = "id1";
        Timestamp timestamp = Timestamp.valueOf("2007-09-23 10:10:10.0");
        String relevant_document = "Y";
        String s3_key = "s3://some-bucket/some-file.pdf";
        doc.setDocument_id(documentID);
        doc.setUploaded_date(timestamp);
        doc.setRelevant_document(relevant_document);
        doc.setS3_key(s3_key);
        // and then gotten
        String actualDocumentID = doc.getDocument_id();
        Timestamp actualTimestamp = doc.getUploaded_date();
        String actualRelevantDocument = doc.getRelevant_document();
        String actualS3Key = doc.getS3_key();


        // then they are as expected
        assertEquals(actualDocumentID, documentID);
        assertEquals(actualTimestamp, timestamp);
        assertEquals(actualRelevantDocument, relevant_document);
        assertEquals(actualS3Key, s3_key);
    }
}
