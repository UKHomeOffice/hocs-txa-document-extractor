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
        String uuid = "00000000-aaaa-bbbb-cccc-000000000000";
        String externalReferenceUuid = "00000000-aaaa-bbbb-cccc-0000000000a1";
        String caseType = "a1";
        String type = "ORIGINAL";
        String pdfLink = "some-file.pdf";
        String status = "UPLOADED";
        Timestamp updatedOn = Timestamp.valueOf("2007-09-23 10:10:10.0");
        Timestamp deletedOn = Timestamp.valueOf("2008-09-23 10:10:10.0");
        String destinationKey = "decs/cs/some/path.pdf";
        String source = "cs";

        doc.setUuid(uuid);
        doc.setExternalReferenceUuid(externalReferenceUuid);
        doc.setCaseType(caseType);
        doc.setType(type);
        doc.setPdfLink(pdfLink);
        doc.setStatus(status);
        doc.setUpdatedOn(updatedOn);
        doc.setDeletedOn(deletedOn);
        doc.setDestinationKey(destinationKey);
        doc.setSource(source);

        // then they are set correctly
        assertEquals(doc.getUuid(), uuid);
        assertEquals(doc.getExternalReferenceUuid(), externalReferenceUuid);
        assertEquals(doc.getCaseType(), caseType);
        assertEquals(doc.getType(), type);
        assertEquals(doc.getPdfLink(), pdfLink);
        assertEquals(doc.getStatus(), status);
        assertEquals(doc.getUpdatedOn(), updatedOn);
        assertEquals(doc.getDeletedOn(), deletedOn);
        assertEquals(doc.getDestinationKey(), destinationKey);
        assertEquals(doc.getSource(), source);
    }

    @Test
    void gettersTest() {
        // given a document
        DocumentRow doc = new DocumentRow();

        // when values are set
        String uuid = "00000000-aaaa-bbbb-cccc-000000000000";
        String externalReferenceUuid = "00000000-aaaa-bbbb-cccc-0000000000a1";
        String caseType = "a1";
        String type = "ORIGINAL";
        String pdfLink = "some-file.pdf";
        String status = "UPLOADED";
        Timestamp updatedOn = Timestamp.valueOf("2007-09-23 10:10:10.0");
        Timestamp deletedOn = Timestamp.valueOf("2008-09-23 10:10:10.0");
        String destinationKey = "decs/cs/some/path.pdf";
        String source = "wcs";

        doc.setUuid(uuid);
        doc.setExternalReferenceUuid(externalReferenceUuid);
        doc.setCaseType(caseType);
        doc.setType(type);
        doc.setPdfLink(pdfLink);
        doc.setStatus(status);
        doc.setUpdatedOn(updatedOn);
        doc.setDeletedOn(deletedOn);
        doc.setDestinationKey(destinationKey);
        doc.setSource(source);

        // and then gotten
        String actualUuid = doc.getUuid();
        String actualExternalReferenceUuid = doc.getExternalReferenceUuid();
        String actualCaseType = doc.getCaseType();
        String actualType = doc.getType();
        String actualPdfLink = doc.getPdfLink();
        String actualStatus = doc.getStatus();
        Timestamp actualUpdatedOn = doc.getUpdatedOn();
        Timestamp actualDeletedOn = doc.getDeletedOn();
        String actualDestinationKey = doc.getDestinationKey();
        String actualSource = doc.getSource();

        // then they are as expected
        assertEquals(actualUuid, uuid);
        assertEquals(actualExternalReferenceUuid, externalReferenceUuid);
        assertEquals(actualCaseType, caseType);
        assertEquals(actualType, type);
        assertEquals(actualPdfLink, pdfLink);
        assertEquals(actualStatus, status);
        assertEquals(actualUpdatedOn, updatedOn);
        assertEquals(actualDeletedOn, deletedOn);
        assertEquals(actualDestinationKey, destinationKey);
        assertEquals(actualSource, source);
    }
}
