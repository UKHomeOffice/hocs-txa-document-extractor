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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@SpringBatchTest
@ActiveProfiles("integration")
public class IngestScenario2Test {
    /*
    Integration Test Ingest Scenario 2

    The purpose of this scenario is to simulate a failure during the initial sql query in PostgresItemReader
    to determine which documents to collect. To force an error here, this scenario does not use TestUtils.setUpPostgres
    because it intentionally creates a table with incorrect schema to force the sql error.

    The expected outcome of the Job is failure with the timestamp unchanged and 0 records
    published to Kafka.
    - 2 records should be in the target s3 bucket (1 for each of the ingest/delete timestamps)
     */
    private static final Logger log = LoggerFactory.getLogger(
        IngestScenario2Test.class);
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
        log.info("Creating schema / table on database with incorrect table definition");
        this.jdbcTemplate.execute("CREATE SCHEMA metadata;");
        String createTable = """
            CREATE TABLE metadata.document_metadata  (
                id bigint,
                WRONG_COLUMN uuid,
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
        this.jdbcTemplate.execute(createTable);

        log.info("Inserting mock data into database");
        String insertRecords = """
            INSERT INTO metadata.document_metadata (WRONG_COLUMN, external_reference_uuid, type, pdf_link, status, updated_on, deleted, deleted_on)
            VALUES
                ('00000000-aaaa-bbbb-cccc-000000000000', '00000000-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file1.pdf', 'UPLOADED', timestamp '2023-03-22 12:00:00', 'False', NULL),
                ('00000001-aaaa-bbbb-cccc-000000000000', '00000001-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file2.pdf', 'UPLOADED', timestamp '2023-03-22 13:00:00', 'False', NULL),
                ('00000002-aaaa-bbbb-cccc-000000000000', '00000002-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file3.pdf', 'UPLOADED', timestamp '2023-03-22 14:00:00', 'False', NULL),
                ('00000003-aaaa-bbbb-cccc-000000000000', '00000003-aaaa-bbbb-cccc-000000000000', 'ORIGINAL', 'decs-file4.pdf', 'UPLOADED', timestamp '2023-03-22 15:00:00', 'False', NULL),
                ('00000004-aaaa-bbbb-cccc-000000000000', '00000004-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file5.pdf', 'UPLOADED', timestamp '2023-03-22 16:00:00', 'False', NULL),
                ('00000005-aaaa-bbbb-cccc-000000000000', '00000005-aaaa-bbbb-cccc-0000000000a1', 'ORIGINAL', 'decs-file6.pdf', 'UPLOADED', timestamp '2023-03-22 17:00:00', 'False', NULL);
            """;
        this.jdbcTemplate.execute(insertRecords);

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
        assertEquals("2023-03-22 11:59:59", TestUtils.getTimestampFromS3("untrusted-bucket", this.endpointURL, this.deletes));

        List<String> keysConsumed = TestUtils.consumeKafkaMessages(bootstrapServers, ingestTopic, 1);
        assertEquals(0, keysConsumed.size());

        // 2 json files (ingest timestamp + delete timestamp) - no files should have been copied in this scenario
        assertEquals(2, TestUtils.countS3Objects("untrusted-bucket", this.endpointURL));
    }
}
