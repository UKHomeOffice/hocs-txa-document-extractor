package uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.jdbc.core.RowMapper;

public class DocumentRowMapper implements RowMapper<DocumentRow>{
    public DocumentRow mapRow(ResultSet rs, int rowNum) throws SQLException {
        DocumentRow document = new DocumentRow();
        document.setDocument_id(rs.getString("document_id"));
        document.setUploaded_date(rs.getDate("uploaded_date"));
        document.setRelevant_document(rs.getString("relevant_document"));
        document.setS3_key(rs.getString("s3_key"));
        return document;
    }
}