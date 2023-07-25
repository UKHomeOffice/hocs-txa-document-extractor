package uk.gov.digital.ho.hocs.hocstxadocumentextractor.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TestUtils {
    /*
    Class contains methods used by different integration test scenarios
    for setUp, tearDown or making assertions.
     */
    private static final Logger log = LoggerFactory.getLogger(
        uk.gov.digital.ho.hocs.hocstxadocumentextractor.utils.TestUtils.class);

    public static void setUpPostgres(JdbcTemplate jdbcTemplate, String insertIntoQuery) {
        /*
        Execute the specified INSERT INTO query using the specified JdbcTemplate
         */
        log.info("Creating schema / table on database");
        jdbcTemplate.execute("DROP SCHEMA IF EXISTS metadata CASCADE;");
        jdbcTemplate.execute("CREATE SCHEMA metadata;");
        String createTable = """
            CREATE TABLE metadata.document_metadata  (
                id bigint,
                uuid uuid,
                external_reference_uuid uuid,
                type text,
                display_name text,
                file_link text,
                pdf_link text,
                status text,
                created_on timestamp without time zone,
                updated_on timestamp without time zone,
                deleted boolean,
                upload_owner uuid,
                deleted_on timestamp without time zone
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
        log.info("Uploading mock documents to s3://" + s3Bucket);
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

    public static void setUpKafka(AdminClient client, String topicName) throws Exception {
        /*
        Create Kafka topic. Check for existence first.
         */
        log.info("Checking existing Kafka topics...");
        ListTopicsResult listTopics = client.listTopics();
        Set<String> names = listTopics.names().get();
        boolean exists = names.contains(topicName);
        if (!exists) {
            log.info("Creating Kafka topic: " + topicName);
            NewTopic topic = new NewTopic(topicName, 1, (short)1);
            CreateTopicsResult result = client.createTopics(Collections.singleton(topic));
            KafkaFuture<Void> future = result.values().get(topicName);
            future.get();
        }
        else {
            log.info("Kafka topic already exists: " + topicName);
        }
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
        log.info("Deleting mock files from s3://" + s3Bucket);
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        Region region = Region.EU_WEST_2;
        S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(new URI(endpointURL))
            .build();

        // first list all the objects in the s3
        ListObjectsV2Request listObjects = ListObjectsV2Request
            .builder()
            .bucket(s3Bucket)
            .build();

        ListObjectsV2Response res = s3.listObjectsV2(listObjects);
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

    public static void tearDownKafka(AdminClient client, String topicName) throws Exception {
        /*
        Delete kafka topic. Close the client.
         */
        log.info("Deleting Kafka Topic: " + topicName);
        DeleteTopicsResult result = client.deleteTopics(Collections.singleton(topicName));
        KafkaFuture<Void> future = result.values().get(topicName);
        future.get();
        log.info("Closing Kafka client");
        client.close();
    }

    public static String getTimestampFromS3(String s3Bucket, String endpointURL, boolean deletes) throws Exception {
        /*
        Get / parse the timestamp from the metadata.json in S3.
        Used in integration test assertions.
         */
        String metadataFile = deletes ? "decs/cs/deletes.json" : "decs/cs/ingests.json";
        log.info("loading metadata json from S3: " + s3Bucket);
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        Region region = Region.EU_WEST_2;
        S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(new URI(endpointURL))
            .build();

        GetObjectRequest objectRequest = GetObjectRequest
            .builder()
            .key(metadataFile)
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

    public static List<String> consumeKafkaMessages(String bootstrapServers, String topicName, Integer maxNumberOfRecords) {
        /*
        Load the requested number of records from kafka to perform assertions on.
        Return only the keys.
         */
        log.info("Creating Kafka consumer to check messages published...");
        Map<String, Object> conf = new HashMap<>();
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        /*
        Using the same group name repeatedly across tests causes slow re-balancing of consumer
        groups so generate a random string from uuid to use as the consumer group name in each test.
         */
        UUID uuid = UUID.randomUUID();
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, uuid.toString());
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /*
        Since we don't care about the message values for these test assertions, just use a
        StringDeserializer instead of creating a proper custom DocumentDeserializer
         */
        final Consumer<String, String> consumer = new KafkaConsumer<String, String>(conf);
        consumer.subscribe(Collections.singletonList(topicName));

        Integer recordCount = 0;
        Integer attempt = 1;
        Integer maxAttempts = 3;
        List<String> keysConsumed = new ArrayList<String>();
        log.info("Consuming records...");
        while (recordCount < maxNumberOfRecords && attempt <= maxAttempts) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            recordCount = recordCount + consumerRecords.count();
            attempt = attempt + 1;
            consumerRecords.forEach(record -> {
                keysConsumed.add(record.key());
            });
        }
        return keysConsumed;
    }

    public static Integer countS3Objects(String bucketName, String endpointURL) throws Exception {
        /*
        Count the number of objects in the given S3 bucket. Used in integration test assertions.
         */

        log.info("Counting objects in bucket: " + bucketName);
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        Region region = Region.EU_WEST_2;
        S3Client s3 = S3Client.builder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(new URI(endpointURL))
            .build();

        ListObjectsV2Request request = ListObjectsV2Request.builder()
            .bucket(bucketName)
            .maxKeys(100)
            .build();

        ListObjectsV2Response result = s3.listObjectsV2(request);
        return result.keyCount();
    }

}
