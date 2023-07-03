package uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents;

import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;

@Getter @Setter
public class DocumentRow {
    /*
    A custom data structure to contain the information
    related to a single DECS document that this extractor
    cares about.
     */
    private String uuid;
    private String externalReferenceUuid;
    private String caseType;
    private String type;
    private String pdfLink;
    private String status;
    private Timestamp updatedOn;
    private Timestamp deletedOn;
}
