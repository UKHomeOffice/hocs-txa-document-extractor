package uk.gov.digital.ho.hocs.hocstxadocumentextractor;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch.TxaKafkaItemWriter;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.utils.TestUtils;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@SpringBatchTest
@ActiveProfiles("integration")
public class IngestScenario4Test {
    /*
    Integration Test Ingest Scenario 4

    This scenario simulates the case where there is an error during the last chunk of documents
    to process. This scenario creates this error by making the s3_key of the last record one
    that does not exist in the S3. This scenario is an example of the case where it is not the
    first chunk that fails.

    The expected outcome is a failed Job but with the timestamp updated to the latest timestamp of
    the last successful chunk within the Job and 4 records written to Kafka.
    We expect 10 files in the target s3 (4 pdf files + 4 related json files + 2 timestamp files)
     */
    private static final Logger log = LoggerFactory.getLogger(
        IngestScenario4Test.class);
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    private @Value("${mode.delete}") boolean deletes;
    private @Value("${s3.endpoint_url}") String endpointURL;
    private @Value("${kafka.bootstrap_servers}") String bootstrapServers;
    private @Value("${kafka.ingest_topic}") String ingestTopic;
    private JdbcTemplate jdbcTemplate;
    private AdminClient kafkaClient = null;

    @Autowired
    public void setDataSource(@Qualifier("metadataSource") DataSource metadataSource) {
        this.jdbcTemplate = new JdbcTemplate(metadataSource);
    }

    @BeforeEach
    void setUp() throws Exception {
        log.info("Test setUp");
        String insertRecords = """
            INSERT INTO metadata.document_metadata (uuid, external_reference_uuid, type, pdf_link, status, updated_on, deleted_on)
            VALUES
                ('00000000-aaaa-bbbb-cccc-000000000000', '00000000-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', NULL),
                ('00000001-aaaa-bbbb-cccc-000000000000', '00000001-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file2.pdf', 'UPLOADED', timestamp '2023-03-22 13:00:00', NULL),
                ('00000002-aaaa-bbbb-cccc-000000000000', '00000002-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file3.pdf', 'UPLOADED', timestamp '2023-03-22 14:00:00', NULL),
                ('00000003-aaaa-bbbb-cccc-000000000000', '00000003-aaaa-bbbb-cccc-000000000000', 'ORIGINAL', 'decs-file4.pdf', 'UPLOADED', timestamp '2023-03-22 15:00:00', NULL),
                ('00000004-aaaa-bbbb-cccc-000000000000', '00000004-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file5.pdf', 'UPLOADED', timestamp '2023-03-22 16:00:00', NULL),
                ('00000005-aaaa-bbbb-cccc-000000000000', '00000005-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'NONEXISTENT-FILE.pdf', 'UPLOADED', timestamp '2023-03-22 17:00:00', NULL);
            """;
        TestUtils.setUpPostgres(this.jdbcTemplate, insertRecords);

        Path path = Paths.get("src", "integration-test","resources","trusted-s3-data");
        TestUtils.setUpS3(path, "trusted-bucket", this.endpointURL);
        Path otherPath = Paths.get("src", "integration-test","resources","untrusted-s3-data");
        TestUtils.setUpS3(otherPath, "untrusted-bucket", this.endpointURL);

        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaClient = AdminClient.create(conf);
        TestUtils.setUpKafka(kafkaClient, ingestTopic);
    }

    @AfterEach
    void tearDown() throws Exception {
        log.info("Test tearDown");
        TestUtils.tearDownPostgres(this.jdbcTemplate);
        TestUtils.tearDownS3("trusted-bucket", this.endpointURL);
        TestUtils.tearDownS3("untrusted-bucket", this.endpointURL);
        TestUtils.tearDownKafka(kafkaClient, ingestTopic);
    }

    @Test
    public void testJob(@Autowired Job job, @Autowired TxaKafkaItemWriter writer) throws Exception {
        this.jobLauncherTestUtils.setJob(job);
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        writer.commitTimestamp(); // required to trigger the predestroy method during the test
        assertEquals("FAILED", jobExecution.getExitStatus().getExitCode());
        assertEquals("2023-03-22 16:00:00.0", TestUtils.getTimestampFromS3("untrusted-bucket", this.endpointURL, this.deletes));

        List<String> expectedDocs = Arrays.asList("00000000-aaaa-bbbb-cccc-0000000000a1",
                                                  "00000001-aaaa-bbbb-cccc-0000000000a1",
                                                  "00000002-aaaa-bbbb-cccc-0000000000a1",
                                                  "00000004-aaaa-bbbb-cccc-0000000000a1");

        List<String> keysConsumed = TestUtils.consumeKafkaMessages(bootstrapServers, ingestTopic, 10);
        assertEquals(4, keysConsumed.size());
        assertEquals(new HashSet<String>(expectedDocs), new HashSet<String>(keysConsumed));

        // 4 pdf files + 4 related json files + 2 json files (ingest timestamp + delete timestamp)
        assertEquals(10, TestUtils.countS3Objects("untrusted-bucket", this.endpointURL));
    }
}
