package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class JobStartFinishListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobStartFinishListener.class);
    private S3TimestampManager timestampManager;
    private final SlackNotification slackNotification;

    JobStartFinishListener(String targetBucket,
                           String endpointURL,
                           String lastIngest,
                           SlackNotification slackNotification) throws URISyntaxException {
        this.timestampManager = new S3TimestampManager(targetBucket, endpointURL, lastIngest);
        this.slackNotification = slackNotification;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        // Get timestamp
        log.info("Executing beforeJob tasks...");
        String lastSuccessfulCollection = null;
        try {
            lastSuccessfulCollection = this.timestampManager.getTimestamp();
        } catch (IOException e) {
            log.error("Could not parse the metadata.json read from S3, cannot recover.");
            log.error(e.toString());
            System.exit(1);
        }
        log.info("Storing timestamp in job execution context");
        jobExecution.getExecutionContext().putString("lastSuccessfulCollection", lastSuccessfulCollection);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        /*
        This afterJob method runs regardless of the success of the job which is what we want.
        The ItemWriter will only update the StepExecutionContext if events are successfully written out.
        The StepExecutionContext is promoted to JobExecutionContext.
        So it is safe to use the timestamp in the JobExecutionContext even if there is some failure in the Step.
        This afterJob task only gets checkpointTimestamps which have definitely been successfully
        written out by the ItemWriter.
         */
        log.info("Executing afterJob tasks...");
        // Put timestamp
        log.info("Getting last recorded checkpoint from JobExecutionContext");
        String checkpointTimestamp = jobExecution.getExecutionContext().getString("lastSuccessfulCollection");
        boolean success;
        try {
            success = this.timestampManager.putTimestamp(checkpointTimestamp);
        } catch (JsonProcessingException e) {
            log.error("Could not create an updated metadata.json, cannot recover");
            log.error(e.toString());
            success = false;
        }
        if (success) {
            log.info("checkpointTimestamp successfully updated to: " + checkpointTimestamp);
            jobExecution.getExecutionContext().putString("alreadyCommitted", "true");  // to avoid duplication in PreDestroy of the writer
        }
        else {
            log.error("committing the checkpointTimestamp failed");
            log.error("the next execution of the job will reprocesses uncommitted records");
        }

        // Notify
        LocalDateTime startTime = jobExecution.getStartTime();
        LocalDateTime endTime = jobExecution.getEndTime();
        long readCount = jobExecution.getExecutionContext().getLong("readCount");
        log.info("Started=" + startTime);
        log.info("Finished=" + endTime);
        log.info("readCount=" + readCount);
        long noOfMillis = ChronoUnit.MILLIS.between(startTime,endTime);
        double noOfSeconds = noOfMillis / 1000.0;
        double docsPerSecond = readCount / noOfSeconds;
        log.info(String.format("docs/seconds~%.2f", docsPerSecond));
        log.info("Sending job outcome notifications...");

        if(jobExecution.getStatus() == BatchStatus.COMPLETED && success) {
            /*
            All records successfully read, processed, written
            AND the timestamp of the latest record successfully updated in S3
             */
            log.info("Job outcome [SUCCESS]");
            String successMessage = this.slackNotification.craftSuccessMessage(readCount, noOfSeconds, checkpointTimestamp);
            this.slackNotification.publishMessage(successMessage, "txa");
        }
        else {
            String outcome = jobExecution.getStatus().toString();
            log.error("Job outcome [" + outcome + "]");
            log.error("Timestamp commit successful? [" + success + "]");

            String failureMessage = this.slackNotification.craftFailureMessage(outcome, success, checkpointTimestamp);
            this.slackNotification.publishMessage(failureMessage, "decs");
            this.slackNotification.publishMessage(failureMessage, "txa");
        }
    }
}
