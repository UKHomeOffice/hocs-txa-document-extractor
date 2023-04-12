package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentRow;

public class DocumentItemWriter implements ItemWriter<DocumentRow> {
    /*
    Temporary class to support prototyping without attempting
    to write to a real output device.
     */
    private StepExecution stepExecution;
    private static final Logger log = LoggerFactory.getLogger(
        uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch.DocumentItemWriter.class);

    @Override
    public void write(Chunk<? extends DocumentRow> doc_list) {
        String checkpointTimestamp = null;
        for (DocumentRow d : doc_list) {
            log.info("Mock publish of event for doc=" + d.getS3_key() + " with timestamp=" + d.getUploaded_date().toString());
            checkpointTimestamp = d.getUploaded_date().toString();
        }
        /*
        In practice, only update the checkpoint if the write is 100% confirmed successful.
        This is us committing the progress of the job.
        i.e. only after successful flush of all kafka message deliveries
         */
        log.info("Updating checkpointTimestamp in StepContext with " + checkpointTimestamp);
        ExecutionContext stepContext = this.stepExecution.getExecutionContext();
        stepContext.putString("lastSuccessfulCollection", checkpointTimestamp);
    }

    @BeforeStep
    public void saveStepExecution(StepExecution stepExecution) {
        /*
        Makes the step Execution available to the write method so it can be updated
        as chunks are processed.
         */
        this.stepExecution = stepExecution;
    }
}
