package uk.gov.digital.ho.hocs.hocstxadocumentextractor.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.DirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {
    /*
    Class contains methods used by numerous integration test scenarios
    for setUp, tearDown or making assertions.
     */
    private static final Logger log = LoggerFactory.getLogger(
        uk.gov.digital.ho.hocs.hocstxadocumentextractor.utils.TestUtils.class);

    public static void setUpPostgres(JdbcTemplate jdbcTemplate, String insertIntoQuery) {
        /*
        Execute the specified INSERT INTO query using the specified JdbcTemplate
         */
        log.info("Creating schema / table on database");
        jdbcTemplate.execute("CREATE SCHEMA metadata;");
        String createTable = """
            CREATE TABLE metadata.document_metadata  (
                document_id varchar(64),
                uploaded_date timestamp without time zone,
                relevant_document varchar(1),
                s3_key varchar(1024)
            );
            """;
        jdbcTemplate.execute(createTable);

        log.info("Inserting mock data into database");
        jdbcTemplate.execute(insertIntoQuery);
    }

    public static void setUpS3(Path path, String s3Bucket, String endpointURL) throws Exception {
        /*
        Upload files in the specified directory to the specified S3 bucket using the specified endpoint.
         */
        log.info("Uploading mock documents to S3: " + s3Bucket);
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        Region region = Region.EU_WEST_2;
        S3AsyncClient s3 = S3AsyncClient.crtBuilder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(new URI(endpointURL))
            .build();

        S3TransferManager transferManager = S3TransferManager.builder()
            .s3Client(s3)
            .build();

        log.info("Attempting to upload files in in " + path.toAbsolutePath().toString() + ":");
        File file = new File(path.toString());
        for (File f : file.listFiles()) {
            log.info(f.toString());
        }

        UploadDirectoryRequest request = UploadDirectoryRequest.builder()
            .bucket(s3Bucket)
            .source(path)
            .build();

        DirectoryUpload directoryUpload = transferManager.uploadDirectory(request);
        // Wait for the transfer to complete
        CompletedDirectoryUpload completedDirectoryUpload = directoryUpload.completionFuture().join();
        // Log any failed uploads
        completedDirectoryUpload.failedTransfers().forEach(failure -> log.error(failure.toString()));
    }

    public static void tearDownPostgres(JdbcTemplate jdbcTemplate) {
        /*
        Delete all files in the specified S3 bucket
         */
        log.info("Deleting mock data from database");
        jdbcTemplate.execute("DROP SCHEMA IF EXISTS metadata CASCADE;");
    }

    public static void tearDownS3(String s3Bucket, String endpointURL) throws Exception {
        /*
        Delete all files in the specified S3 bucket using the specified endpointURL
         */
        log.info("Deleting mock files from S3");
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        Region region = Region.EU_WEST_2;
        S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(new URI(endpointURL))
            .build();

        // first list all the objects in the s3
        ListObjectsRequest listObjects = ListObjectsRequest
            .builder()
            .bucket(s3Bucket)
            .build();

        ListObjectsResponse res = s3.listObjects(listObjects);
        List<S3Object> objects = res.contents();
        ArrayList<ObjectIdentifier> keys = new ArrayList<>();
        ObjectIdentifier objectId;
        for (S3Object myObj : objects) {
            objectId = ObjectIdentifier.builder()
                .key(myObj.key())
                .build();
            keys.add(objectId);
        }

        // then pass the list to the Delete builder
        Delete del = Delete.builder()
            .objects(keys)
            .build();

        DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
            .bucket(s3Bucket)
            .delete(del)
            .build();

        s3.deleteObjects(multiObjectDeleteRequest);
    }

    public static String getTimestampFromS3(String s3Bucket, String endpointURL) throws Exception {
        /*
        Get / parse the timestamp from the metadata.json in S3.
        Used in integration test assertions.
         */
        log.info("loading metadata.json from S3: " + s3Bucket);
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        Region region = Region.EU_WEST_2;
        S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(new URI(endpointURL))
            .build();

        GetObjectRequest objectRequest = GetObjectRequest
            .builder()
            .key("metadata.json")
            .bucket(s3Bucket)
            .build();

        ResponseInputStream fullObject = s3.getObject(objectRequest);

        Map<String, String> metadata = null;
        try {
            metadata = new ObjectMapper().readValue(fullObject, HashMap.class);
        }
        catch (IOException e) {
            log.error(e.toString());
            throw e;
        }
        String result = metadata.get("lastSuccessfulCollection");
        return result;
    }

}
