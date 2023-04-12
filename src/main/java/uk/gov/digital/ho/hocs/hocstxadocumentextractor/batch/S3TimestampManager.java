package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class S3TimestampManager {
    /*
    Responsible for getting/reading/writing/putting the metadata record
    on the S3 bucket which defines the timestamp of the last successful
    ingested or deleted document.
     */
    private static final Logger log = LoggerFactory.getLogger(
        S3TimestampManager.class);

    private String targetBucket;
    private URI endpointURL;
    private String lastIngest;
    private S3Client s3Client;
    private Map<String, String> metadataJson;

    S3TimestampManager(String targetBucket,
                       String endpointURL,
                       String lastIngest) throws URISyntaxException {
        log.info("Constructing S3TimestampManager to GET/PUT timestamp metadata from/in: " + targetBucket);

        this.targetBucket = targetBucket;
        try {
            this.endpointURL = new URI(endpointURL);
        } catch (URISyntaxException e){
            log.error(e.toString());
            throw e;
        }
        this.lastIngest = lastIngest;

        EnvironmentVariableCredentialsProvider credentialsProvider = EnvironmentVariableCredentialsProvider.create();
        Region region = Region.EU_WEST_2;
        S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(this.endpointURL)
            .build();
        this.s3Client = s3;
    }

    public String getTimestamp() throws IOException {
        if (this.lastIngest != "") {
            log.info("$METADATA_LAST_INGEST is set => Overriding S3 timestamp with local environment variable");
            log.info("$METADATA_LAST_INGEST=" + this.lastIngest);
            return this.lastIngest;
        }

        log.info("Attempting to GET the metadata.json file...");
        GetObjectRequest objectRequest = GetObjectRequest
            .builder()
            .key("metadata.json")
            .bucket(this.targetBucket)
            .build();
        ResponseInputStream fullObject = this.s3Client.getObject(objectRequest);

        Map<String, String> metadata = readJsonBytes(fullObject);
        String result = metadata.get("lastSuccessfulCollection");
        log.info("getTimestamp result is: " + result);
        this.metadataJson = metadata;
        return result;
    }

    public boolean putTimestamp(String checkpointTimestamp) throws JsonProcessingException {
        log.info("Attempting to PUT the updated metadata.json file...");
        log.info("Got timestamp=" + checkpointTimestamp);

        PutObjectRequest objectRequest = PutObjectRequest.builder()
            .bucket(this.targetBucket)
            .key("metadata.json")
            .build();

        this.metadataJson.put("lastSuccessfulCollection", checkpointTimestamp);

        byte[] requestBody = writeJsonBytes(this.metadataJson);

        PutObjectResponse objectResponse = this.s3Client.putObject(objectRequest, RequestBody.fromBytes(requestBody));
        if (objectResponse.sdkHttpResponse().isSuccessful()) {
            return true;
        }
        return false;
    }

    private Map<String, String> readJsonBytes(InputStream inputStream) throws IOException {
        Map<String, String> metadata = null;
        try {
            metadata = new ObjectMapper().readValue(inputStream, HashMap.class);
        }
        catch (IOException e) {
            log.error(e.toString());
            throw e;
        }
        return metadata;
    }

    private byte[] writeJsonBytes(Map<String, String> inputMap) throws JsonProcessingException {
        byte[] bytes = null;
        try {
            bytes = new ObjectMapper().writeValueAsBytes(inputMap);
        }
        catch (JsonProcessingException e) {
            log.error(e.toString());
            throw e;
        }
        return bytes;
    }

}
