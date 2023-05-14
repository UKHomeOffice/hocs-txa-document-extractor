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
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch.DocumentItemWriter;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.utils.TestUtils;

import javax.sql.DataSource;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@SpringBatchTest
@ActiveProfiles("integration")
public class Scenario1Test {
    /*
    Integration Test Scenario 1

    This scenario is one where 5/6 total documents should be collected and all
    successfully processed by the batch job.

    The expected outcome is a successfully completed Job with the timestamp updated
    to that of the latest record in the mock data set.
     */
    private static final Logger log = LoggerFactory.getLogger(
        uk.gov.digital.ho.hocs.hocstxadocumentextractor.Scenario1Test.class);
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
        TestUtils.setUpPostgres(this.jdbcTemplate, insertRecords);

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
    public void testJob(@Autowired Job job, @Autowired DocumentItemWriter writer) throws Exception {
        this.jobLauncherTestUtils.setJob(job);
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        writer.commitTimestamp(); // required to trigger the predestroy method during the test
        assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
        assertEquals("2023-03-22 17:00:00.0", TestUtils.getTimestampFromS3("untrusted-bucket", this.endpointURL));
    }
}
