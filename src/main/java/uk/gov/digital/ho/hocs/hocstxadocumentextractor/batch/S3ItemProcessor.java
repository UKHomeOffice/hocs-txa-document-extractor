package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentRow;

import java.net.URI;
import java.net.URISyntaxException;

public class S3ItemProcessor implements ItemProcessor<DocumentRow, DocumentRow> {
    private static final Logger log = LoggerFactory.getLogger(
        uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch.S3ItemProcessor.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private String sourceBucket;
    private String targetBucket;
    private URI endpointURL;
    private S3Client s3Client;

    S3ItemProcessor(String sourceBucket, String targetBucket, String endpointURL) throws URISyntaxException {
        /*
        Responsible for copying files between two S3 buckets.
         */
        log.info("Constructing S3ItemProcessor to transfer objects from: " + sourceBucket + " to: " + targetBucket);

        this.sourceBucket = sourceBucket;
        this.targetBucket = targetBucket;
        try {
            this.endpointURL = new URI(endpointURL);
        } catch (URISyntaxException e){
            log.error(e.toString());
            throw e;
        }

        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        Region region = Region.EU_WEST_2;
        S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(this.endpointURL)
            .build();
        this.s3Client = s3;
    }

    @Override
    public DocumentRow process(final DocumentRow doc) throws S3Exception, JsonProcessingException {
        final String pdfLink = doc.getPdfLink();
        log.info("Processing document: " + pdfLink);

        log.info("Copying document");
        copyBucketObject(this.s3Client, this.sourceBucket, pdfLink, this.targetBucket);

        log.info("Creating metadata json");
        // don't attempt to replace possible existing .pdf extension to avoid handling cases
        // where it might not exist
        final String jsonLink = pdfLink + ".json";
        byte[] metadataPayload = this.objectMapper.writeValueAsBytes(doc);

        log.info("Uploading metadata json");
        putBucketObject(this.s3Client, metadataPayload, jsonLink, this.targetBucket);

        return doc;
    }

    public static String copyBucketObject (S3Client s3, String fromBucket, String objectKey, String toBucket) {

        CopyObjectRequest copyReq = CopyObjectRequest.builder()
            .sourceBucket(fromBucket)
            .sourceKey(objectKey)
            .destinationBucket(toBucket)
            .destinationKey(objectKey)
            .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)
            .build();

        try {
            CopyObjectResponse copyResponse = s3.copyObject(copyReq);
            return copyResponse.copyObjectResult().toString();

        } catch (S3Exception e) {
            log.error(e.awsErrorDetails().errorMessage());
            throw e;
        }

    }

    public static String putBucketObject (S3Client s3, byte[] requestBody, String objectKey, String toBucket) {

        PutObjectRequest objectRequest = PutObjectRequest.builder()
            .bucket(toBucket)
            .key(objectKey)
            .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)
            .build();

        PutObjectResponse putResponse = s3.putObject(objectRequest, RequestBody.fromBytes(requestBody));
        return putResponse.toString();
    }

}
