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
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@SpringBatchTest
@ActiveProfiles("integration")
public class IngestQueryTest {
    /*
    IngestQuery Test Scenario

    While other Ingest Scenarios implicitly test the query used to collect documents
    for ingest, this test scenario focuses on this part and tests it to a greater extent.

    This scenario uploads a wider range of mock data to the postgres RDS to test edge cases on
    the PostgresItemReader.
     */
    private static final Logger log = LoggerFactory.getLogger(
        IngestQueryTest.class);
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
            INSERT INTO metadata.document_metadata (uuid, external_reference_uuid, type, pdf_link, status, updated_on, deleted, deleted_on)
            VALUES
                ('00000000-aaaa-bbbb-cccc-000000000000', '00000000-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000001-aaaa-bbbb-cccc-000000000000', '00000001-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file1.pdf', 'NOTUPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000002-aaaa-bbbb-cccc-000000000000', '00000002-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', NULL, 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000003-aaaa-bbbb-cccc-000000000000', '00000003-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'True', timestamp '2023-03-22 12:01:00'),
                ('00000004-aaaa-bbbb-cccc-000000000000', '00000004-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 11:59:59', 'False', NULL),
                ('00000005-aaaa-bbbb-cccc-000000000000', '00000005-aaaa-bbbb-cccc-0000000000a1', 'OTHERTYPE', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000006-aaaa-bbbb-cccc-000000000000', '00000006-aaaa-bbbb-cccc-0000000000a2', 'CONTRIBUTION', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000007-aaaa-bbbb-cccc-000000000000', '00000007-aaaa-bbbb-cccc-0000000000a3', 'ORIGINAL', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000008-aaaa-bbbb-cccc-000000000000', '00000008-aaaa-bbbb-cccc-0000000000a4', 'Original Complaint', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000009-aaaa-bbbb-cccc-000000000000', '00000009-aaaa-bbbb-cccc-0000000000a5', 'Contribution Response', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000010-aaaa-bbbb-cccc-000000000000', '00000010-aaaa-bbbb-cccc-0000000000a6', 'ORIGINAL', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000011-aaaa-bbbb-cccc-000000000000', '00000011-aaaa-bbbb-cccc-0000000000c5', 'Complaint leaflet', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000012-aaaa-bbbb-cccc-000000000000', '00000012-aaaa-bbbb-cccc-0000000000c7', 'Original complaint', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000013-aaaa-bbbb-cccc-000000000000', '00000013-aaaa-bbbb-cccc-0000000000d1', 'Initial Correspondence', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000014-aaaa-bbbb-cccc-000000000000', '00000014-aaaa-bbbb-cccc-0000000000ff', 'ORIGINAL', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000015-aaaa-bbbb-cccc-000000000000', '00000015-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'True', NULL);
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

        List<String> expectedDocs = Arrays.asList("00000000-aaaa-bbbb-cccc-0000000000a1",
                                                  "00000006-aaaa-bbbb-cccc-0000000000a2",
                                                  "00000007-aaaa-bbbb-cccc-0000000000a3",
                                                  "00000008-aaaa-bbbb-cccc-0000000000a4",
                                                  "00000009-aaaa-bbbb-cccc-0000000000a5",
                                                  "00000011-aaaa-bbbb-cccc-0000000000c5",
                                                  "00000012-aaaa-bbbb-cccc-0000000000c7",
                                                  "00000013-aaaa-bbbb-cccc-0000000000d1");

        assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
        List<String> keysConsumed = TestUtils.consumeKafkaMessages(bootstrapServers, ingestTopic, 10);
        assertEquals(8, keysConsumed.size()); // assert 5 records were written
        // assert the 5 expected document id's were written (use HashSet to ignore order)
        assertEquals(new HashSet<String>(expectedDocs), new HashSet<String>(keysConsumed));

        // 1 pdf files + 1 related json files (because all pdfLinks in the mock data point to the same file)
        // + 2 json files (ingest timestamp + delete timestamp)
        assertEquals(4, TestUtils.countS3Objects("untrusted-bucket", this.endpointURL));
    }
}
