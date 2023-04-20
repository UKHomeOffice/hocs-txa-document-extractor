package uk.gov.digital.ho.hocs.hocstxadocumentextractor;

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
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.utils.TestUtils;

import javax.sql.DataSource;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@SpringBatchTest
@ActiveProfiles("integration")
public class Scenario2Test {
    /*
    Integration Test Scenario 2

    The purpose of this scenario is to simulate a failure during the initial sql query in PostgresItemReader
    to determine which documents to collect. To force an error here, this scenario does not use TestUtils.setUpPostgres
    because it intentionally creates a table with incorrect schema to force the sql error.

    The expected outcome of the Job is failure with the timestamp unchanged.
     */
    private static final Logger log = LoggerFactory.getLogger(
        Scenario2Test.class);
    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    private @Value("${s3.endpoint_url}") String endpointURL;
    private JdbcTemplate jdbcTemplate;

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
                document_id varchar(64),
                uploaded_date timestamp without time zone,
                WRONG_COLUMN varchar(1),
                s3_key varchar(1024)
            );
            """;
        this.jdbcTemplate.execute(createTable);

        log.info("Inserting mock data into database");
        String insertRecords = """
            INSERT INTO metadata.document_metadata
            VALUES
                ('a1', timestamp '2023-03-22 12:00:00', 'Y', 'decs-file1.pdf'),
                ('b2', timestamp '2023-03-22 13:00:00', 'Y', 'decs-file2.pdf'),
                ('c3', timestamp '2023-03-22 14:00:00', 'Y', 'decs-file3.pdf'),
                ('d4', timestamp '2023-03-22 15:00:00', 'N', 'decs-file4.pdf'),
                ('e5', timestamp '2023-03-22 16:00:00', 'Y', 'decs-file5.pdf'),
                ('f6', timestamp '2023-03-22 17:00:00', 'Y', 'decs-file6.pdf');
            """;
        this.jdbcTemplate.execute(insertRecords);

        Path path = FileSystems.getDefault().getPath("src", "integration-test","resources","trusted-s3-data");
        TestUtils.setUpS3(path, "trusted-bucket", this.endpointURL);
        Path otherPath = FileSystems.getDefault().getPath("src", "integration-test","resources","untrusted-s3-data");
        TestUtils.setUpS3(otherPath, "untrusted-bucket", this.endpointURL);
    }

    @AfterEach
    void tearDown() throws Exception {
        log.info("Test tearDown");
        TestUtils.tearDownPostgres(this.jdbcTemplate);
        TestUtils.tearDownS3("trusted-bucket", this.endpointURL);
        TestUtils.tearDownS3("untrusted-bucket", this.endpointURL);
    }

    @Test
    public void testJob(@Autowired Job job) throws Exception {
        this.jobLauncherTestUtils.setJob(job);
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertEquals("FAILED", jobExecution.getExitStatus().getExitCode());
        assertEquals("2023-03-22 11:59:59", TestUtils.getTimestampFromS3("untrusted-bucket", this.endpointURL));
    }
}
