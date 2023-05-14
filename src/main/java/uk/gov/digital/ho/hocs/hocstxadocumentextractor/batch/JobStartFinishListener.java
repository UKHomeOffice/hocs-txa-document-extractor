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

        The commit of the timestamp occurs in the PreDestroy method of the ItemWriter class instead
        of here so that we can be sure it is attempted even if the job is interrupted.

        This method is responsible for success and failure notifications.
         */
        log.info("Executing afterJob tasks...");

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