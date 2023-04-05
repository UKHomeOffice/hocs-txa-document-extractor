package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentRow;

import java.util.Date;

public class DocumentItemProcessor implements ItemProcessor<DocumentRow, DocumentRow> {
    private static final Logger log = LoggerFactory.getLogger(
        uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch.DocumentItemProcessor.class);

    @Override
    public DocumentRow process(final DocumentRow doc) throws Exception {
        final String id = doc.getDocument_id();
        final Date date = doc.getUploaded_date();
        final String relevant = doc.getRelevant_document();
        final String s3_key = doc.getS3_key();

        log.info("Mock processing of document " + id);
        log.info("Uploaded: " + date);
        log.info("Relevant: " + relevant);
        log.info("S3 Key: " + s3_key);

        return doc;
    }

}
