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

@SpringBootTest(properties = {"mode.delete=true"})
@SpringBatchTest
@ActiveProfiles("integration")
public class DeleteQueryTest {
    /*
    DeleteQuery Test Scenario

    While other Delete Scenarios implicitly test the query used to collect documents
    for deletion, this test scenario focuses on this part and tests it to a greater extent.

    This scenario uploads a wider range of mock data to the postgres RDS to test edge cases on
    the PostgresItemReader.
     */
    private static final Logger log = LoggerFactory.getLogger(
        DeleteQueryTest.class);
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    private @Value("${mode.delete}") boolean deletes;
    private @Value("${s3.endpoint_url}") String endpointURL;
    private @Value("${kafka.bootstrap_servers}") String bootstrapServers;
    private @Value("${kafka.delete_topic}") String deleteTopic;
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
                ('00000000-aaaa-bbbb-cccc-000000000000', '00000000-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', timestamp '2023-03-22 12:01:00'),
                ('00000001-aaaa-bbbb-cccc-000000000000', '00000001-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-15 12:00:00', timestamp '2023-03-15 12:00:00'),
                ('00000002-aaaa-bbbb-cccc-000000000000', '00000002-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-14 12:00:00', timestamp '2023-03-14 12:00:00'),
                ('00000003-aaaa-bbbb-cccc-000000000000', '00000003-aaaa-bbbb-cccc-0000000000e1', 'Email', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', timestamp '2023-03-27 12:00:00');
            """;
        TestUtils.setUpPostgres(this.jdbcTemplate, insertRecords);

        Path path = Paths.get("src", "integration-test","resources","trusted-s3-data");
        TestUtils.setUpS3(path, "trusted-bucket", this.endpointURL);
        Path otherPath = Paths.get("src", "integration-test","resources","untrusted-s3-data");
        TestUtils.setUpS3(otherPath, "untrusted-bucket", this.endpointURL);

        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaClient = AdminClient.create(conf);
        TestUtils.setUpKafka(kafkaClient, deleteTopic);
    }

    @AfterEach
    void tearDown() throws Exception {
        log.info("Test tearDown");
        TestUtils.tearDownPostgres(this.jdbcTemplate);
        TestUtils.tearDownS3("trusted-bucket", this.endpointURL);
        TestUtils.tearDownS3("untrusted-bucket", this.endpointURL);
        TestUtils.tearDownKafka(kafkaClient, deleteTopic);
    }

    @Test
    public void testJob(@Autowired Job job, @Autowired TxaKafkaItemWriter writer) throws Exception {
        this.jobLauncherTestUtils.setJob(job);
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        writer.commitTimestamp(); // required to trigger the predestroy method during the test

        List<String> expectedDocs = Arrays.asList("00000000-aaaa-bbbb-cccc-0000000000a1",
                                                  "00000001-aaaa-bbbb-cccc-0000000000a1",
                                                  "00000003-aaaa-bbbb-cccc-0000000000e1");

        assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
        List<String> keysConsumed = TestUtils.consumeKafkaMessages(bootstrapServers, deleteTopic, 10);
        assertEquals(3, keysConsumed.size()); // assert 5 records were written
        // assert the 5 expected document id's were written (use HashSet to ignore order)
        assertEquals(new HashSet<String>(expectedDocs), new HashSet<String>(keysConsumed));
    }
}
