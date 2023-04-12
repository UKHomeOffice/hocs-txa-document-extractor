package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentRow;

import java.io.IOException;
import java.net.URISyntaxException;

public class DocumentItemWriter implements ItemWriter<DocumentRow> {
    /*
    Temporary class to support prototyping without attempting
    to write to a real output device.
     */
    private static final Logger log = LoggerFactory.getLogger(
        uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch.DocumentItemWriter.class);
    private StepExecution stepExecution;
    private String targetBucket;
    private String endpointURL;

    DocumentItemWriter(String targetBucket, String endpointURL) {
        this.targetBucket = targetBucket;
        this.endpointURL = endpointURL;
    }

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

    @PreDestroy
    public void lastGasp() {
        /*
        The purpose of this method is to commit the last successful timestamp in the case when there is
        an unexpected interruption of the job. e.g. SIGTERM signal from Kubernetes.
        To avoid triggering this in the case of a normal shutdown of the writer Bean, it is conditioned to
        look for a key in the JobExecutionContext which will be true if the normal Listener has already
        successfully committed the final updated timestamp.
        If it has not already been committed, this method tries to do so before shutdown.

        I think this is necessary to occur here because on an unexpected interruption e.g. SIGTERM,
        other beans such as the JobStartFinishListener will start getting destroyed immediately.
        The PromotionListener (to move the timestamp from step to job) will not run.
        This class is the one place where an accurate last successful timestamp can be retrieved to attempt
        to commit it before the application exits.
         */
        String alreadyCommitted = this.stepExecution.getJobExecution().getExecutionContext().getString("alreadyCommitted", "false");
        String lastCheckpointTimestamp = this.stepExecution.getExecutionContext().getString("lastSuccessfulCollection", "null");
        if (alreadyCommitted != "true" && lastCheckpointTimestamp != "null") {
            try{
                log.info("Trying to commit a timestamp before closing...");
                S3TimestampManager timestampManager = new S3TimestampManager(this.targetBucket,
                    this.endpointURL,
                    lastCheckpointTimestamp);
                timestampManager.getTimestamp();
                boolean success = timestampManager.putTimestamp(lastCheckpointTimestamp);
                if (success) {
                    log.info("Timestamp committed successfully during PreDestroy");
                }
            } catch (URISyntaxException | IOException e) {
                log.error(e.toString());
            }
        } else {
            log.info("DocumentItemWriter PreDestroy skipped because timestamp has already been committed or is null");
        }
    }
}
