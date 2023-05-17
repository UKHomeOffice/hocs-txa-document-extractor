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
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@SpringBatchTest
@ActiveProfiles("integration")
public class Scenario1Test {
    /*
    Integration Test Scenario 1

    This scenario is one where 5/6 total documents should be collected and all
    successfully processed by the batch job. 1 document is not collected due to it
    not meeting the criteria set out in the PostgresItemReader query.

    The expected outcome is:
    - a successfully completed Job
    - the timestamp updated to that of the latest record in the mock data set
    - 5 records published to kafka topic
     */
    private static final Logger log = LoggerFactory.getLogger(
        uk.gov.digital.ho.hocs.hocstxadocumentextractor.Scenario1Test.class);
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
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
                ('00000001-aaaa-bbbb-cccc-000000000000', '00000000-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file2.pdf', 'UPLOADED', timestamp '2023-03-22 13:00:00', NULL),
                ('00000002-aaaa-bbbb-cccc-000000000000', '00000000-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file3.pdf', 'UPLOADED', timestamp '2023-03-22 14:00:00', NULL),
                ('00000003-aaaa-bbbb-cccc-000000000000', '00000000-aaaa-bbbb-cccc-000000000000', 'ORIGINAL', 'decs-file4.pdf', 'UPLOADED', timestamp '2023-03-22 15:00:00', NULL),
                ('00000004-aaaa-bbbb-cccc-000000000000', '00000000-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file5.pdf', 'UPLOADED', timestamp '2023-03-22 16:00:00', NULL),
                ('00000005-aaaa-bbbb-cccc-000000000000', '00000000-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file6.pdf', 'UPLOADED', timestamp '2023-03-22 17:00:00', NULL);
            """;
        TestUtils.setUpPostgres(this.jdbcTemplate, insertRecords);

        Path path = FileSystems.getDefault().getPath("src", "integration-test","resources","trusted-s3-data");
        TestUtils.setUpS3(path, "trusted-bucket", this.endpointURL);
        Path otherPath = FileSystems.getDefault().getPath("src", "integration-test","resources","untrusted-s3-data");
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

        List<String> expectedDocs = Arrays.asList("00000000-aaaa-bbbb-cccc-000000000000",
                                                  "00000001-aaaa-bbbb-cccc-000000000000",
                                                  "00000002-aaaa-bbbb-cccc-000000000000",
                                                  "00000004-aaaa-bbbb-cccc-000000000000",
                                                  "00000005-aaaa-bbbb-cccc-000000000000");

        assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
        assertEquals("2023-03-22 17:00:00.0", TestUtils.getTimestampFromS3("untrusted-bucket", this.endpointURL));
        List<String> keysConsumed = TestUtils.consumeKafkaMessages(bootstrapServers, ingestTopic, 10);
        assertEquals(5, keysConsumed.size()); // assert 5 records were written
        // assert the 5 expected document id's were written (use HashSet to ignore order)
        assertEquals(new HashSet<String>(expectedDocs), new HashSet<String>(keysConsumed));
    }
}
