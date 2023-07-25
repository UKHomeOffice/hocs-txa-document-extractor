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
    private static final Logger log = LoggerFactory.getLogger(SlackNotification.class);
    private Map<String, String> slackURLMap;
    protected Slack slack;
    protected boolean deletes;
    protected String hocsSystem;

    SlackNotification(Map<String, String> slackURLMap, boolean deletes, String hocsSystem) {
        this.slackURLMap = slackURLMap;
        this.deletes = deletes;
        this.slack = Slack.getInstance();
        this.hocsSystem = hocsSystem;
    }

    public String craftSuccessMessage(long readCount, double noOfSeconds) {
        String mode = this.deletes ? "Collection for Delete" : "Collection for Ingest";
        String successTemplate = """
            DECS -> TXA $mode Successful ($hocsSystem).
            $readCount documents ingested in $noOfSeconds seconds.""";

        final String successPayload = successTemplate
            .replace("$mode", mode)
            .replace("$hocsSystem", this.hocsSystem)
            .replace("$readCount", "" + readCount)
            .replace("$noOfSeconds", "" + noOfSeconds);
        return successPayload;
    }

    public String craftFailureMessage(String outcome) {
        String mode = this.deletes ? "Collection for Delete" : "Collection for Ingest";
        String failureTemplate = """
            DECS -> TXA $mode Failed ($hocsSystem).
            Outcome was $outcome.""";

        final String failurePayload = failureTemplate
            .replace("$mode", mode)
            .replace("$hocsSystem", this.hocsSystem)
            .replace("$outcome", outcome);
        return failurePayload;
    }

    public String craftTimestampMessage(boolean success, String lastCheckpointTimestamp) {
        String mode = this.deletes ? "for deletes" : "for ingests";
        String timestampTemplate = """
            lastSuccessfulCollection $mode = $timestamp ($hocsSystem)
            timestamp commit successful? $success.
            """;

        final String timestampPayload = timestampTemplate
            .replace("$mode", mode)
            .replace("$hocsSystem", this.hocsSystem)
            .replace("$timestamp", lastCheckpointTimestamp)
            .replace("$success", "" + success); // converts bool to string
        return timestampPayload;
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
        } else { // do not throw an exception if the Slack notification fails
            log.error("Notification to " + channelSelector + " failed");
            log.error("Response code was " + responseCode);
            log.error(response.getBody());
        }

        return responseCode;
    }
}
