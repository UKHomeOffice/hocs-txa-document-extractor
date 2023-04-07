package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentRow;

public class DocumentItemWriter implements ItemWriter<DocumentRow> {
    /*
    Temporary class to support prototyping without attempting
    to write to a real output device.
     */
    @Override
    public void write(Chunk<? extends DocumentRow> doc_list) {
        for (DocumentRow d : doc_list) {
            System.out.println("Mock publishing of event for document " + d.getS3_key());
        }
    }
}