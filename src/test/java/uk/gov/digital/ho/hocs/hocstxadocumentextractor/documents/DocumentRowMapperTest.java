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
        DocumentRowMapper docMap = new DocumentRowMapper();
        // Some values for a DocumentRow
        String documentID = "id1";
        Timestamp timestamp = Timestamp.valueOf("2007-09-23 10:10:10.0");
        String relevantDocument = "Y";
        String s3Key = "s3://some-bucket/some-file.pdf";
        // And a ResultSet
        when(resultSet.getString("document_id")).thenReturn(documentID);
        when(resultSet.getTimestamp("uploaded_date")).thenReturn(timestamp);
        when(resultSet.getString("relevant_document")).thenReturn(relevantDocument);
        when(resultSet.getString("s3_key")).thenReturn(s3Key);


        // when the mapper is called on the resultset
        DocumentRow doc = docMap.mapRow(resultSet, 1);

        // then it returns a DocumentRow object
        assertEquals(doc.getDocument_id(), documentID);
        assertEquals(doc.getUploaded_date(), timestamp);
        assertEquals(doc.getRelevant_document(), relevantDocument);
        assertEquals(doc.getS3_key(), s3Key);
    }

}
