package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
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
    Responsible for reading/writing the record on the target S3 bucket which defines the
    timestamp of the last successfully ingested and last successfully deleted documents.
     */
    private static final Logger log = LoggerFactory.getLogger(
        S3TimestampManager.class);

    protected String targetBucket;
    protected URI endpointURL;
    protected String lastCollection;
    protected boolean deletes;
    protected String metadataPath;
    protected S3Client s3Client;
    protected Map<String, String> metadataJson;

    S3TimestampManager(String targetBucket,
                       String endpointURL,
                       String lastCollection,
                       boolean deletes,
                       String hocsSystem) throws URISyntaxException {
        log.info("Constructing S3TimestampManager to manage timestamps in: " + targetBucket);
        log.info("S3 endpoint: " + endpointURL);

        this.targetBucket = targetBucket;
        try {
            this.endpointURL = new URI(endpointURL);
        } catch (URISyntaxException e){
            log.error(e.toString());
            throw e;
        }
        this.lastCollection = lastCollection;
        this.deletes = deletes;
        String metadataFile = deletes ? "deletes.json" : "ingests.json";
        // delete and ingest timestamps for cs and wcs are all tracked independently
        this.metadataPath = String.format("decs/%s/%s", hocsSystem.toLowerCase(), metadataFile);

        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        Region region = Region.EU_WEST_2;
        S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(this.endpointURL)
            .build();
        this.s3Client = s3;
    }

    public String getTimestamp() throws IOException {
        log.info("Attempting to get the last successful collection timestamp from " + this.targetBucket + "/" + this.metadataPath);
        GetObjectRequest objectRequest = GetObjectRequest
            .builder()
            .key(this.metadataPath)
            .bucket(this.targetBucket)
            .build();
        ResponseInputStream fullObject = this.s3Client.getObject(objectRequest);

        Map<String, String> metadata = readJsonBytes(fullObject);
        String result = metadata.get("lastSuccessfulCollection");
        log.info("lastSuccessfulCollection is: " + result);
        this.metadataJson = metadata;

        if (this.lastCollection != "") {
            /*
            This occurs after loading the metadata.json from S3 even if the env var is set so that
            the hashmap can be constructed from the real metadata.json file. Only then is the value
            in the hashmap overwritten with the value from the environment variable.
             */
            log.info("lastSuccessfulCollection is set to be overridden => Overriding S3 timestamp with local value");
            log.info("lastSuccessfulCollection=" + this.lastCollection);
            metadata.put("lastSuccessfulCollection", this.lastCollection);
            return this.lastCollection;
        }

        return result;
    }

    public boolean putTimestamp(String checkpointTimestamp) throws JsonProcessingException {
        log.info("Attempting to put the updated last successful collection timestamp...");
        log.info("Updating to: " + checkpointTimestamp);

        PutObjectRequest objectRequest = PutObjectRequest.builder()
            .bucket(this.targetBucket)
            .key(this.metadataPath)
            .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)  // required for owner of target bucket to control the file.
            .build();

        this.metadataJson.put("lastSuccessfulCollection", checkpointTimestamp);

        byte[] requestBody = writeJsonBytes(this.metadataJson);

        PutObjectResponse objectResponse = this.s3Client.putObject(objectRequest, RequestBody.fromBytes(requestBody));
        if (objectResponse.sdkHttpResponse().isSuccessful()) {
            log.info("Update successful");
            return true;
        }
        return false;
    }

    protected Map<String, String> readJsonBytes(InputStream inputStream) throws IOException {
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

    protected byte[] writeJsonBytes(Map<String, String> inputMap) throws JsonProcessingException {
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
