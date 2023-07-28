package uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.xml.transform.Result;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DocumentRowMapperTest {

    @Mock
    ResultSet resultSet;

    @Test
    void rowMapTest() throws SQLException {
        // given a RowMapper
        DocumentRowMapper docMap = new DocumentRowMapper("cs");
        // And some values for a DocumentRow
        String uuid = "00000000-aaaa-bbbb-cccc-000000000000";
        String externalReferenceUuid = "00000000-aaaa-bbbb-cccc-0000000000a1";
        String caseType = "a1";
        String type = "ORIGINAL";
        String pdfLink = "some-file.pdf";
        String status = "UPLOADED";
        Timestamp updatedOn = Timestamp.valueOf("2007-09-23 10:10:10.0");
        Timestamp deletedOn = Timestamp.valueOf("2008-09-23 10:10:10.0");
        // And a ResultSet
        when(resultSet.getString("uuid")).thenReturn(uuid);
        when(resultSet.getString("external_reference_uuid")).thenReturn(externalReferenceUuid);
        when(resultSet.getString("case_type")).thenReturn(caseType);
        when(resultSet.getString("type")).thenReturn(type);
        when(resultSet.getString("pdf_link")).thenReturn(pdfLink);
        when(resultSet.getString("status")).thenReturn(status);
        when(resultSet.getTimestamp("updated_on")).thenReturn(updatedOn);
        when(resultSet.getTimestamp("deleted_on")).thenReturn(deletedOn);

        // when the mapper is called on the resultset
        DocumentRow doc = docMap.mapRow(resultSet, 1);

        // then it returns a DocumentRow object
        assertEquals(doc.getUuid(), uuid);
        assertEquals(doc.getExternalReferenceUuid(), externalReferenceUuid);
        assertEquals(doc.getCaseType(), caseType);
        assertEquals(doc.getType(), type);
        assertEquals(doc.getPdfLink(), pdfLink);
        assertEquals(doc.getStatus(), status);
        assertEquals(doc.getUpdatedOn(), updatedOn);
        assertEquals(doc.getDeletedOn(), deletedOn);
        assertEquals(doc.getDestinationKey(), "decs/cs/year=2007/month=09/day=23/some-file.pdf");
        assertEquals(doc.getSource(), "cs");
    }

    @Test
    void computeDestinationKeyTest() {
        /*
        Test that the destination key is computed correctly
         */
        DocumentRowMapper docMap = new DocumentRowMapper("cs");
        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2023-12-12 10:10:10.0");
        String pdfLink = "a1-b2-c3/d4-e5-f6.pdf";
        String result = docMap.computeDestinationKey(timestamp, pdfLink);
        String expected = "decs/cs/year=2023/month=12/day=12/a1-b2-c3/d4-e5-f6.pdf";
        assertEquals(expected, result);
    }

    @Test
    void computeDestinationKeyZeroPaddingTest() {
        /*
        Test that the destination key zero-pads singe digit months and days
         */
        DocumentRowMapper docMap = new DocumentRowMapper("wcs");
        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2023-1-9 10:10:10.0");
        String pdfLink = "a1-b2-c3/d4-e5-f6.pdf";
        String result = docMap.computeDestinationKey(timestamp, pdfLink);
        String expected = "decs/wcs/year=2023/month=01/day=09/a1-b2-c3/d4-e5-f6.pdf";
        assertEquals(expected, result);

    }

}
