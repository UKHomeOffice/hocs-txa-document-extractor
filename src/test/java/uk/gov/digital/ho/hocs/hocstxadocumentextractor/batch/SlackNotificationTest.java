package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import com.slack.api.Slack;
import com.slack.api.webhook.Payload;
import com.slack.api.webhook.WebhookResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SlackNotificationTest {

    @Test
    public void craftSuccessMessageTest() {
        /*
        Test a success message is generated from parameters as expected
         */
        Map<String, String> slackURLMap = new HashMap<String, String>();
        slackURLMap.put("txa", "txaSlackURL");
        SlackNotification slackNotification = new SlackNotification(slackURLMap);

        long readCount = 5;
        double noOfSeconds = 0.555;
        String expected = """
            DECS -> TXA Ingest Successful.
            5 documents ingested in 0.555 seconds.""";

        String actual = slackNotification.craftSuccessMessage(readCount, noOfSeconds);
        assertEquals(expected, actual);
    }

    @Test
    public void craftFailureMessageTest() {
        /*
        Test a failure message is generated from parameters as expected.
         */
        Map<String, String> slackURLMap = new HashMap<String, String>();
        slackURLMap.put("txa", "txaSlackURL");
        SlackNotification slackNotification = new SlackNotification(slackURLMap);

        String outcome = "FAILED";
        String expected = """
            DECS -> TXA Ingest Failed.
            Outcome was FAILED.""";

        String actual = slackNotification.craftFailureMessage(outcome);
        assertEquals(expected, actual);
    }

    @Test
    public void craftTimestampMessageTest() {
        /*
        Test a timestamp message is generated from parameters as expected.
         */
        Map<String, String> slackURLMap = new HashMap<String, String>();
        slackURLMap.put("txa", "txaSlackURL");
        SlackNotification slackNotification = new SlackNotification(slackURLMap);

        boolean success = true;
        String timestamp = "2023-03-22 11:59:59.0";
        String expected = """
            lastSuccessfulTimestamp 2023-03-22 11:59:59.0
            commit successful: true.
            """;

        String actual = slackNotification.craftTimestampMessage(success, timestamp);
        assertEquals(expected, actual);
    }

    @Test public void getWebhookURLTest() {
        /*
        Test values are retrieved from the webhook url hashmap correctly
         */
        Map<String, String> slackURLMap = new HashMap<String, String>();
        slackURLMap.put("txa", "txaSlackURL");
        SlackNotification slackNotification = new SlackNotification(slackURLMap);

        String expected = "txaSlackURL";
        String actual = slackNotification.getWebhookURL("txa");
        assertEquals(expected, actual);
    }

    @Test
    public void publishMessageEmptyURLTest() {
        /*
        Test a HTTP400 is returned if webhook URL is empty string ""
         */
        Map<String, String> slackURLMap = new HashMap<String, String>();
        slackURLMap.put("txa", "");
        SlackNotification slackNotification = new SlackNotification(slackURLMap);

        int expected = 400;
        int responseCode = slackNotification.publishMessage("any message", "txa");
        assertEquals(expected, responseCode);
    }

    @Test
    public void publishMessageNullURLTest() {
        /*
        Test a HTTP400 is returned if webhook URL is empty null
        e.g. when the requested channelSelector does not exist in the slackURLMap
         */
        Map<String, String> slackURLMap = new HashMap<String, String>();
        slackURLMap.put("txa", "txaSlackURL");
        SlackNotification slackNotification = new SlackNotification(slackURLMap);

        int expected = 400;
        int responseCode = slackNotification.publishMessage("any message", "NONEXISTENT_KEY");
        assertEquals(expected, responseCode);
    }

    @Test
    public void publishMessageExceptionTest() throws IOException {
        /*
        Test a HTTP500 is returned if there is an IOException in the slack.send attempt
         */
        Map<String, String> slackURLMap = new HashMap<String, String>();
        slackURLMap.put("txa", "txaSlackURL");
        SlackNotification slackNotification = new SlackNotification(slackURLMap);

        Slack mockSlack = mock(Slack.class);
        when(mockSlack.send(any(String.class), any(Payload.class))).thenThrow(IOException.class);
        slackNotification.slack = mockSlack;

        int expected = 500;
        int actual = slackNotification.publishMessage("any", "txa");
        assertEquals(expected, actual);
    }

    @Test
    public void publishMessageReturnCodeTest() throws IOException {
        /*
        Test the return code of the slack.send call is returned by our publishMessage method
         */
        Map<String, String> slackURLMap = new HashMap<String, String>();
        slackURLMap.put("txa", "txaSlackURL");
        SlackNotification slackNotification = new SlackNotification(slackURLMap);

        Slack mockSlack = mock(Slack.class);
        WebhookResponse mockWebhookResponse = mock(WebhookResponse.class);
        when(mockSlack.send(any(String.class), any(Payload.class))).thenReturn(mockWebhookResponse);
        when(mockWebhookResponse.getCode()).thenReturn(200);
        slackNotification.slack = mockSlack;

        int expected = 200;
        int actual = slackNotification.publishMessage("any", "txa");
        assertEquals(expected, actual);
    }
}
