package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

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
    private boolean deletes;

    JobStartFinishListener(String targetBucket,
                           String endpointURL,
                           String lastIngest,
                           String lastDelete,
                           boolean deletes,
                           SlackNotification slackNotification) throws URISyntaxException {
        this.timestampManager = new S3TimestampManager(targetBucket, endpointURL, lastIngest, lastDelete, deletes);
        this.slackNotification = slackNotification;
        this.deletes = deletes;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        /*
        Load last successful collection timestamp from the target S3.
         */
        String mode = this.deletes ? "DELETE" : "INGEST";
        log.info(String.format("Application is running in %s mode.", mode));
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

        The commit of the timestamp occurs in the PreDestroy method of the ItemWriter class instead
        of here so that we can be sure it is attempted even if the job is interrupted.

        This method is responsible for computing some runtime stats and success and failure notifications.
         */
        log.info("Executing afterJob tasks...");

        // Compute & log some runtime stats
        LocalDateTime startTime = jobExecution.getStartTime();
        LocalDateTime endTime = jobExecution.getEndTime();
        long readCount = jobExecution.getExecutionContext().getLong("readCount");
        log.info("Runtime Statistics:");
        log.info("Started at " + startTime);
        log.info("Finished at " + endTime);
        log.info("Number of documents processed was " + readCount);
        long noOfMillis = ChronoUnit.MILLIS.between(startTime,endTime);
        double noOfSeconds = noOfMillis / 1000.0;
        double docsPerSecond = readCount / noOfSeconds;
        log.info(String.format("docs/seconds ~ %.2f", docsPerSecond));

        // Notify
        log.info("Sending job outcome notifications...");
        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("Job outcome [SUCCESS]");
            String successMessage = this.slackNotification.craftSuccessMessage(readCount, noOfSeconds);
            this.slackNotification.publishMessage(successMessage, "txa");
        }
        else {
            String outcome = jobExecution.getStatus().toString();
            log.error("Job outcome [" + outcome + "]");

            String failureMessage = this.slackNotification.craftFailureMessage(outcome);
            this.slackNotification.publishMessage(failureMessage, "decs");
            this.slackNotification.publishMessage(failureMessage, "txa");
        }
    }
}
