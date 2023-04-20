package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import com.slack.api.Slack;
import com.slack.api.webhook.WebhookResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.slack.api.webhook.WebhookPayloads.payload;

public class SlackNotification {
    /*
    Responsible for crafting and sending notification messages to Slack.
     */
    private static final Logger log = LoggerFactory.getLogger(
        SlackNotification.class);
    private Map<String, String> slackURLMap;
    protected Slack slack;

    SlackNotification(Map<String, String> slackURLMap) {
        this.slackURLMap = slackURLMap;
        this.slack = Slack.getInstance();
    }

    public String craftSuccessMessage(long readCount, double noOfSeconds, String checkpointTimestamp) {
        String successTemplate = """
            DECS -> TXA Ingest Successful.
            $readCount documents ingested in $noOfSeconds seconds.
            Last successful ingest timestamp now $checkpointTimestamp.""";

        final String successPayload = successTemplate
            .replace("$readCount", "" + readCount)
            .replace("$noOfSeconds", "" + noOfSeconds)
            .replace("$checkpointTimestamp", checkpointTimestamp);
        return successPayload;
    }

    public String craftFailureMessage(String outcome, boolean success, String checkpointTimestamp) {
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
        return failurePayload;
    }

    public String getWebhookURL(String channelSelector) {
        String webhookURL = this.slackURLMap.get(channelSelector);
        return webhookURL;
    }

    public int publishMessage(String payload, String channelSelector) {
        log.info("Processing notification to " + channelSelector + " channel");
        WebhookResponse response = null;
        String webhookURL = this.getWebhookURL(channelSelector);

        if (webhookURL == null || webhookURL == "") {
            log.warn("Webhook URL for " + channelSelector + " is empty");
            log.warn("Skipping attempt to publish " + channelSelector + " notification");
            return 400;
        }

        try {
            response = this.slack.send(webhookURL, payload(p -> p.text(payload)));
        } catch (IOException e) {
            log.error(e.toString());
            return 500;
        }

        int responseCode = response.getCode();
        if (responseCode == 200) {
            log.info("Notification to " + channelSelector + " sent successfully");
        } else {
            log.error("Notification to " + channelSelector + " failed");
            log.error("Response code was " + responseCode);
            log.error(response.getBody());
        }

        return responseCode;
    }
}
