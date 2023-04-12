package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slack.api.Slack;
import com.slack.api.webhook.WebhookResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static com.slack.api.webhook.WebhookPayloads.payload;

public class JobStartFinishListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(JobStartFinishListener.class);
    private S3TimestampManager timestampManager;
    private final String decsSlackURL;
    private final String txaSlackURL;

    JobStartFinishListener(String targetBucket,
                           String endpointURL,
                           String lastIngest,
                           String decsSlackURL,
                           String txaSlackURL) throws URISyntaxException {
        this.timestampManager = new S3TimestampManager(targetBucket, endpointURL, lastIngest);
        this.decsSlackURL = decsSlackURL;
        this.txaSlackURL = txaSlackURL;
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
        This afterJob task therefore only gets checkpointTimestamps which have definitely been successfully
        written out by the ItemWriter.
         */
        log.info("Executing afterJob tasks...");
        Slack slack = Slack.getInstance();
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
            jobExecution.getExecutionContext().putString("alreadyCommitted", "true");
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

            String successTemplate = """
                DECS -> TXA Ingest Successful.
                $readCount documents ingested in $noOfSeconds seconds.
                Last successful ingest timestamp now $checkpointTimestamp.""";

            final String successPayload = successTemplate
                .replace("$readCount", "" + readCount)
                .replace("$noOfSeconds", "" + noOfSeconds)
                .replace("$checkpointTimestamp", checkpointTimestamp);

            // Text Analytics channel only
            WebhookResponse response = null;
            try {
                response = slack.send(this.txaSlackURL, payload(p -> p.text(successPayload)));
            } catch (IOException e) {
                log.error(e.toString());
            }
            if (response.getCode() == 200) {
                log.info("TXA notification sent successfully");
            } else {
                log.error("TXA notification failed to send");
                log.error("Response code was " + response.getCode());
                log.error(response.getBody());
            }
        }
        else {
            String outcome = jobExecution.getStatus().toString();
            log.error("Job outcome [" + outcome + "]");
            log.error("Commit successful [" + success + "]");

            String failureTemplate = """
                DECS -> TXA Ingest Failed.
                Outcome was $outcome.
                Timestamp commit successful - $success ($timestamp).
                """;

            String timestamp = success ? checkpointTimestamp : "unsuccessful";

            final String failurePayload = failureTemplate
                .replace("$outcome", outcome)
                .replace("$success", "" + success)
                .replace("$timestamp", timestamp);

            // Text Analytics channel and DECS channel
            WebhookResponse decs_response = null;
            try {
                decs_response = slack.send(this.decsSlackURL, payload(p -> p.text(failurePayload)));
            } catch (IOException e) {
                log.error(e.toString());
            }
            if (decs_response.getCode() == 200) {
                log.info("DECS notification sent successfully");
            } else {
                log.error("DECS notification failed to send");
                log.error("Response code was " + decs_response.getCode());
                log.error(decs_response.getBody());
            }

            WebhookResponse txa_response = null;
            try {
                txa_response = slack.send(this.txaSlackURL, payload(p -> p.text(failurePayload)));
            } catch (IOException e) {
                log.error(e.toString());
            }
            if (txa_response.getCode() == 200) {
                log.info("TXA notification sent successfully");
            } else {
                log.error("TXA notification failed to send");
                log.error("Response code was " + txa_response.getCode());
                log.error(txa_response.getBody());
            }
        }
    }
}
