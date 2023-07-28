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
    /*
    This class is responsible for copying files between the source and target buckets.
    It copies documents identified by the PostgresItemReader.
    For each document copied, it also creates a json file on the target bucket containing
    the relevant metadata for that document.
     */
    private static final Logger log = LoggerFactory.getLogger(
        uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch.S3ItemProcessor.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private String sourceBucket;
    private String targetBucket;
    private URI endpointURL;
    private S3Client s3Client;

    S3ItemProcessor(String sourceBucket, String targetBucket, String endpointURL) throws URISyntaxException {
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
        final String sourceKey = doc.getPdfLink();
        final String destinationKey = doc.getDestinationKey();
        // don't attempt to replace possible existing .pdf extension to avoid handling cases
        // where it might not exist
        final String jsonKey = destinationKey + ".json";
        log.info("Processing document with externalReferenceUuid " + doc.getExternalReferenceUuid());

        log.debug("Copying document");
        copyBucketObject(this.s3Client, this.sourceBucket, sourceKey, this.targetBucket, destinationKey);

        log.debug("Creating & uploading metadata json");
        byte[] metadataPayload = this.objectMapper.writeValueAsBytes(doc);
        putBucketObject(this.s3Client, metadataPayload, jsonKey, this.targetBucket);

        return doc;
    }

    public static String copyBucketObject (S3Client s3, String fromBucket, String sourceKey, String toBucket, String destinationKey) {

        CopyObjectRequest copyReq = CopyObjectRequest.builder()
            .sourceBucket(fromBucket)
            .sourceKey(sourceKey)
            .destinationBucket(toBucket)
            .destinationKey(destinationKey)
            .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)  // required for owner of target bucket to control the file.
            // see https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl
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
            .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)  // required for owner of target bucket to control the file.
            // see https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl
            .build();

        PutObjectResponse putResponse = s3.putObject(objectRequest, RequestBody.fromBytes(requestBody));
        return putResponse.toString();
    }

}
