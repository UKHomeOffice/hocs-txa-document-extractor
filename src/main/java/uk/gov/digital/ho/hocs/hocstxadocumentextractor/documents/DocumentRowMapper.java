package uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.StringJoiner;

import org.springframework.jdbc.core.RowMapper;

public class DocumentRowMapper implements RowMapper<DocumentRow>{
    /*
    Maps rows from the ResultSet of the PostgresItemReader to instances
    of the DocumentRow class.
     */
    private static final DateTimeFormatter yearFormatter = DateTimeFormatter.ofPattern("yyyy");
    private static final DateTimeFormatter monthFormatter = DateTimeFormatter.ofPattern("MM");
    private static final DateTimeFormatter dayFormatter = DateTimeFormatter.ofPattern("dd");
    public String hocsSystem;

    public DocumentRowMapper(String hocsSystem) {
        this.hocsSystem = hocsSystem;
    }


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

        String destinationKey = computeDestinationKey(document.getUpdatedOn(), document.getPdfLink());
        document.setDestinationKey(destinationKey);

        document.setSource(this.hocsSystem);

        return document;
    }

    public String computeDestinationKey(Timestamp updatedOn, String pdfLink) {
        /*
        Craft the path where objects will be written to in the destination S3 bucket.

        Partition by year, month, day of the updatedOn date.
         */
        LocalDateTime dateTime = updatedOn.toLocalDateTime();
        String year = "year=" + dateTime.format(yearFormatter);
        String month = "month=" + dateTime.format(monthFormatter);
        String day = "day=" + dateTime.format(dayFormatter);

        StringJoiner joiner = new StringJoiner("/");
        // example destinationKey: decs/cs/year=2023/month=07/day=28/pdfLink
        joiner.add("decs").add(this.hocsSystem).add(year).add(month).add(day).add(pdfLink);
        return joiner.toString();
    }
}
