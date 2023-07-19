package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.core.Response;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class S3TimestampManagerTest {

    @Test
    public void constructorTest() throws URISyntaxException {
        /*
        Test the class can instantiate with AWS credentials set as environment variables
        and a valid S3 Endpoint URI.
         */
        boolean deletes = false;
        S3TimestampManager timestampManager = new S3TimestampManager("bucket",
            "http://endpoint.url",
            "2023-04-03 10:10:10.0",
            "",
            deletes);
        assertNotNull(timestampManager.targetBucket);
        assertNotNull(timestampManager.endpointURL);
        assertNotNull(timestampManager.lastIngest);
        assertNotNull(timestampManager.lastDelete);
        assertEquals(timestampManager.metadataFile, "decs/ingests.json");
        assertNotNull(timestampManager.s3Client);
    }

    @Test
    public void constructorBadURITest() throws URISyntaxException {
        /*
        Test the class constructor throws an exception when provided an invalid URI
         */
        boolean deletes = false;
        assertThrows(URISyntaxException.class, () -> new S3TimestampManager(
            "bucket",
            "some:bad && url",
            "2023-04-03 10:10:10.0",
            "",
            deletes));
    }

    @Test
    public void getTimestampNoOverrideTest() throws URISyntaxException, IOException {
        /*
        Test the getTimestamp method uses the timestamp from S3 when an empty lastIngest
        argument is given in the constructor.
         */

        // Mock the S3 response to a predictable timestamp value
        boolean deletes = false;
        S3TimestampManager timestampManager = new S3TimestampManager("bucket",
            "http://endpoint.url",
            "",
            "",
            deletes);
        S3Client mockClient = mock(S3Client.class);
        String mockJson = """
            {"lastSuccessfulCollection": "2023-04-13 10:10:10.0"}""";
        Response responseType = mock(Response.class);
        InputStream mockResponse = new ByteArrayInputStream(mockJson.getBytes());
        ResponseInputStream response = new ResponseInputStream(responseType, mockResponse);
        when(mockClient.getObject(any(GetObjectRequest.class))).thenReturn(response);

        // inject mock
        timestampManager.s3Client = mockClient;

        // check result
        String result = timestampManager.getTimestamp();
        assertEquals("2023-04-13 10:10:10.0", result);
    }

    @Test
    public void getTimestampWithOverrideTest() throws URISyntaxException, IOException {
        /*
        Test the getTimestamp method overrides the timestamp from s3 when a timestamp is
        given in the lastIngest constructor argument.
         */
        // Mock the S3 response to a predictable timestamp value
        boolean deletes = false;
        S3TimestampManager timestampManager = new S3TimestampManager("bucket",
            "http://endpoint.url",
            "2023-01-01 00:00:00",
            "",
            deletes);
        S3Client mockClient = mock(S3Client.class);
        String mockJson = """
            {"lastSuccessfulCollection": "2023-04-13 10:10:10.0"}""";
        Response responseType = mock(Response.class);
        InputStream mockResponse = new ByteArrayInputStream(mockJson.getBytes());
        ResponseInputStream response = new ResponseInputStream(responseType, mockResponse);
        when(mockClient.getObject(any(GetObjectRequest.class))).thenReturn(response);

        // inject mock
        timestampManager.s3Client = mockClient;

        // check result
        String result = timestampManager.getTimestamp();
        assertEquals("2023-01-01 00:00:00", result);
    }

    @Test
    public void putTimestampSuccessTest() throws URISyntaxException, IOException {
        /*
        Test putTimestamp returns true when successful put request is made
         */
        // Mock the putObject response of the S3 Client
        boolean deletes = false;
        S3TimestampManager timestampManager = new S3TimestampManager("bucket",
            "http://endpoint.url",
            "2023-01-01 00:00:00",
            "",
            deletes);
        S3Client mockClient = mock(S3Client.class);
        PutObjectResponse mockPutResponse = mock(PutObjectResponse.class);
        SdkHttpResponse mockSdkResponse = mock(SdkHttpResponse.class);
        Map<String, String> metadataJson = new HashMap<>();

        when(mockSdkResponse.isSuccessful()).thenReturn(true);
        when(mockPutResponse.sdkHttpResponse()).thenReturn(mockSdkResponse);
        when(mockClient.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(mockPutResponse);

        // inject mock
        timestampManager.s3Client = mockClient;
        timestampManager.metadataJson = metadataJson;

        // check result
        boolean result = timestampManager.putTimestamp("2023-01-01 00:00:00");
        assertEquals(true, result);
    }

    @Test
    public void putTimestampFailTest() throws URISyntaxException, IOException {
        /*
        Test putTimestamp returns false when put request fails
         */
        // Mock the putObject response of the S3 Client
        boolean deletes = false;
        S3TimestampManager timestampManager = new S3TimestampManager("bucket",
            "http://endpoint.url",
            "2023-01-01 00:00:00",
            "",
            deletes);
        S3Client mockClient = mock(S3Client.class);
        PutObjectResponse mockPutResponse = mock(PutObjectResponse.class);
        SdkHttpResponse mockSdkResponse = mock(SdkHttpResponse.class);
        Map<String, String> metadataJson = new HashMap<>();

        when(mockSdkResponse.isSuccessful()).thenReturn(false);
        when(mockPutResponse.sdkHttpResponse()).thenReturn(mockSdkResponse);
        when(mockClient.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(mockPutResponse);

        // inject mock
        timestampManager.s3Client = mockClient;
        timestampManager.metadataJson = metadataJson;

        // check result
        boolean result = timestampManager.putTimestamp("2023-01-01 00:00:00");
        assertEquals(false, result);

    }

    @Test
    public void readJsonBytesValidTest() throws URISyntaxException, IOException {
        /*
        Test readJsonBytes converts a valid InputStream to a valid HashMap
         */
        // Mock an acceptable input to the readJsonBytes method
        boolean deletes = false;
        S3TimestampManager timestampManager = new S3TimestampManager("bucket",
            "http://endpoint.url",
            "2023-01-01 00:00:00",
            "",
            deletes);

        String mockJson = """
            {"lastSuccessfulCollection": "2023-04-13 10:10:10.0"}""";
        InputStream mockInputStream = new ByteArrayInputStream(mockJson.getBytes());

        Map<String, String> expected = new HashMap<>();
        expected.put("lastSuccessfulCollection", "2023-04-13 10:10:10.0");

        // call method
        Map<String, String> actual = timestampManager.readJsonBytes(mockInputStream);

        // check equal expected HashMap
        assertEquals(expected, actual);
    }

    @Test
    public void readJsonBytesInvalidTest() throws URISyntaxException {
        /*
        Test readJsonBytes throws an exception when provided invalid json to parse
         */
        // Mock an malformed input to the readJsonBytes method
        boolean deletes = false;
        S3TimestampManager timestampManager = new S3TimestampManager("bucket",
            "http://endpoint.url",
            "2023-01-01 00:00:00",
            "",
            deletes);

        String malformedJson = """
            {"lastSuccessfulCollection 2023-04-13 10:10:10.0"}""";
        InputStream mockInputStream = new ByteArrayInputStream(malformedJson.getBytes());

        Map<String, String> expected = new HashMap<>();
        expected.put("lastSuccessfulCollection", "2023-04-13 10:10:10.0");

        // check method throws exception
        assertThrows(IOException.class,
            () -> timestampManager.readJsonBytes(mockInputStream));
    }

    @Test
    public void writeJsonBytesValidTest() throws JsonProcessingException, URISyntaxException {
        /*
        Test writeJsonBytes converts a valid HashMap to byte array
         */
        // Mock an acceptable input to the writeJsonBytes method
        boolean deletes = false;
        S3TimestampManager timestampManager = new S3TimestampManager("bucket",
            "http://endpoint.url",
            "2023-01-01 00:00:00",
            "",
            deletes);

        Map<String, String> input = new HashMap<>();
        input.put("lastSuccessfulCollection", "2023-04-13 10:10:10.0");

        String example = """
            {"lastSuccessfulCollection":"2023-04-13 10:10:10.0"}""";
        byte[] expected = example.getBytes();

        // call method
       byte[] actual = timestampManager.writeJsonBytes(input);

        // check equal expected HashMap
        assertTrue(Arrays.equals(expected, actual));
    }

}
