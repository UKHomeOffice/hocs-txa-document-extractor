package uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents;

import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;

@Getter @Setter
public class DocumentRow {
    /*
    A custom data structure to contain the information
    related to a single DECS document

    TODO: Add extra fields here once the metadata structure is known.
     */
    private String document_id;
    private Timestamp uploaded_date;
    private String relevant_document;
    private String s3_key;

}
