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
import java.util.HashMap;
import java.util.Map;

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
    private String txaSlackURL;
    private String decsSlackURL;

    DocumentItemWriter(String targetBucket, String endpointURL, String txaSlackURL, String decsSlackURL) {
        this.targetBucket = targetBucket;
        this.endpointURL = endpointURL;
        this.txaSlackURL = txaSlackURL;
        this.decsSlackURL = decsSlackURL;
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
    public void commitTimestamp() {
        /*
        The purpose of this method is to commit the timestamp of the last successful document sent
        to the text analytics pipeline. This is defined here (instead of the JobStartFinishListener)
        to ensure that it is always invoked - whether the job finishes successfully or the job is
        interrupted (for example by a Kubernetes SIGTERM signal).

        I think this is necessary to occur here because on an unexpected interruption e.g. SIGTERM,
        other beans such as the JobStartFinishListener will start getting destroyed immediately.
        The PromotionListener (to move the timestamp from step to job) will not run.
        This class is the one place where an accurate last successful timestamp can be retrieved to attempt
        to commit it before the application exits.

        This method creates new class instances (rather than using Beans created by Spring) because
        pre-existing beans will already be destroyed before this class can utilise them.
         */
        String lastCheckpointTimestamp = this.stepExecution.getExecutionContext().getString("lastSuccessfulCollection", "empty");
        if (lastCheckpointTimestamp == "empty") {
            log.info("Timestamp is null in ExecutionContext so committing it is skipped.");
            return;
        }

        try{
            log.info("Trying to commit the last successful timestamp...");
            S3TimestampManager timestampManager = new S3TimestampManager(this.targetBucket,
                this.endpointURL,
                lastCheckpointTimestamp);

            timestampManager.getTimestamp();
            boolean success = timestampManager.putTimestamp(lastCheckpointTimestamp);

            Map<String, String> slackURLMap = new HashMap<String, String>();
            slackURLMap.put("txa", this.txaSlackURL);
            slackURLMap.put("decs", this.decsSlackURL);
            SlackNotification slackNotification = new  SlackNotification(slackURLMap);
            String timestampMessage = slackNotification.craftTimestampMessage(success, lastCheckpointTimestamp);

            if (success) {
                log.info("Timestamp committed successfully during PreDestroy");
                slackNotification.publishMessage(timestampMessage, "txa");
            }
            else {
                log.error("committing the checkpointTimestamp failed");
                log.error("the next execution of the job will reprocesses uncommitted records");
                slackNotification.publishMessage(timestampMessage, "txa");
                slackNotification.publishMessage(timestampMessage, "decs");
            }
        } catch (URISyntaxException | IOException e) {
            log.error(e.toString());
        }
    }
}
