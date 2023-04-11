package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class JobStartFinishListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobStartFinishListener.class);
    private S3TimestampManager timestampManager;

    JobStartFinishListener(String targetBucket,
                           String endpointURL,
                           String lastIngest) {
        this.timestampManager = new S3TimestampManager(targetBucket, endpointURL, lastIngest);
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        // Get timestamp
        log.info("Executing beforeJob tasks...");
        String lastSuccessfulCollection = this.timestampManager.getTimestamp();
        log.info("Storing timestamp in job execution context");
        jobExecution.getExecutionContext().putString("lastSuccessfulCollection", lastSuccessfulCollection);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        /*
        This afterJob method runs regardless of the success of the job which is what we want.
        The ItemWriter will only update the StepExecutionContext if events are successfully written out.
        The StepExecutionContext is promoted to JobExecutionContext.
        This afterJob task therefore only gets checkpointTimestamps which have definitely been successfully
        written out by the ItemWriter.
         */
        log.info("Executing afterJob tasks...");
        // Put timestamp
        log.info("Getting last recorded checkpoint from JobExecutionContext");
        String checkpointTimestamp = jobExecution.getExecutionContext().getString("lastSuccessfulCollection");
        boolean success = this.timestampManager.putTimestamp(checkpointTimestamp);
        if (success) {
            log.info("checkpointTimestamp successfully updated to: " + checkpointTimestamp);
        }
        else {
            log.error("committing the checkpointTimestamp failed");
        }

        // Notify
        log.info("Sending job outcome notifications...");
        // TODO: Add slack notifications
        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
            LocalDateTime startTime = jobExecution.getStartTime();
            LocalDateTime endTime = jobExecution.getEndTime();
            long readCount = jobExecution.getExecutionContext().getLong("readCount");
            log.info("Job outcome [SUCCESS]");
            log.info("Started=" + startTime);
            log.info("Finished=" + endTime);
            log.info("readCount=" + readCount);
            long noOfMillis = ChronoUnit.MILLIS.between(startTime,endTime);
            double noOfSeconds = noOfMillis / 1000.0;
            double docsPerSecond = readCount / noOfSeconds;
            log.info(String.format("docs/seconds~%.2f", docsPerSecond));
        }
        else {
            String outcome = jobExecution.getStatus().toString();
            log.error("Job outcome [" + outcome + "]");
        }
    }
}
