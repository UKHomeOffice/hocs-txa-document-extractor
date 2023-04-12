package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
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

    private String sourceBucket;
    private String targetBucket;
    private URI endpointURL;
    private S3Client s3Client;

    S3ItemProcessor(String sourceBucket, String targetBucket, String endpointURL){
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
            cthrow e;
        }

        EnvironmentVariableCredentialsProvider credentialsProvider = EnvironmentVariableCredentialsProvider.create();
        Region region = Region.EU_WEST_2;
        S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(this.endpointURL)
            .build();
        this.s3Client = s3;
    }

    @Override
    public DocumentRow process(final DocumentRow doc) throws S3Exception {
        final String s3_key = doc.getS3_key();
        log.info("Copying document: " + s3_key);

        copyBucketObject(this.s3Client, this.sourceBucket, s3_key, this.targetBucket);
        return doc;
    }

    public static String copyBucketObject (S3Client s3, String fromBucket, String objectKey, String toBucket) {

        CopyObjectRequest copyReq = CopyObjectRequest.builder()
            .sourceBucket(fromBucket)
            .sourceKey(objectKey)
            .destinationBucket(toBucket)
            .destinationKey(objectKey)
            .build();

        try {
            CopyObjectResponse copyRes = s3.copyObject(copyReq);
            return copyRes.copyObjectResult().toString();

        } catch (S3Exception e) {
            log.error(e.awsErrorDetails().errorMessage());
            throw e;
        }

    }
}
