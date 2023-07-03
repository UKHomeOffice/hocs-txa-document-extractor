package uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.jdbc.core.RowMapper;

public class DocumentRowMapper implements RowMapper<DocumentRow>{
    /*
    Maps rows from the ResultSet of the PostgresItemReader to instances
    of the DocumentRow class.
     */
    public DocumentRow mapRow(ResultSet rs, int rowNum) throws SQLException {
        DocumentRow document = new DocumentRow();
        document.setUuid(rs.getString("uuid"));
        document.setExternalReferenceUuid(rs.getString("external_reference_uuid"));
        document.setCaseType(rs.getString("case_type"));
        document.setType(rs.getString("type"));
        document.setPdfLink(rs.getString("pdf_link"));
        document.setStatus(rs.getString("status"));
        document.setUpdatedOn(rs.getTimestamp("updated_on"));
        document.setDeletedOn(rs.getTimestamp("deleted_on"));
        return document;
    }
}
