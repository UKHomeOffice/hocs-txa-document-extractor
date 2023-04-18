package uk.gov.digital.ho.hocs.hocstxadocumentextractor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.DirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.utils.TestUtils;

import javax.sql.DataSource;
import java.io.File;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@SpringBatchTest
@ActiveProfiles("integration")
public class Scenario1Test {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    private JdbcTemplate jdbcTemplate;
    private String endpointURL = "http://s3.localhost.localstack.cloud:4566";

    @Autowired
    public void setDataSource(@Qualifier("metadataSource") DataSource metadataSource) {
        this.jdbcTemplate = new JdbcTemplate(metadataSource);
    }

    @BeforeEach
    void beforeAll() throws Exception {
        // populate postgres database
        System.out.println("--- SOME TEST SETUP ---");
        this.jdbcTemplate.execute("CREATE SCHEMA metadata;");

        String createTable = """
            CREATE TABLE metadata.document_metadata  (
                document_id varchar(64),
                uploaded_date timestamp without time zone,
                relevant_document varchar(1),
                s3_key varchar(1024)
            );
            """;
        this.jdbcTemplate.execute(createTable);

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

        // populate s3 buckets
        DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
        Region region = Region.EU_WEST_2;
        S3AsyncClient s3 = S3AsyncClient.crtBuilder()
            .region(region)
            .credentialsProvider(credentialsProvider)
            .endpointOverride(new URI(this.endpointURL))
            .build();

        S3TransferManager transferManager = S3TransferManager.builder()
            .s3Client(s3)
            .build();

        Path path = FileSystems.getDefault().getPath("src", "integration-test","resources","s3_data");
        System.out.println(path.toAbsolutePath().toString());
        //System.out.println(new File(path.toString()).listFiles());
        File file = new File(path.toString());
        for (File f : file.listFiles()) {
            System.out.println(f);
        }

        UploadDirectoryRequest request = UploadDirectoryRequest.builder()
            .bucket("trusted-bucket")
            .source(path)
            .build();

        System.out.println("Attempting upload of data to bucket...");
        DirectoryUpload directoryUpload = transferManager.uploadDirectory(request);
        // Wait for the transfer to complete
        CompletedDirectoryUpload completedDirectoryUpload = directoryUpload.completionFuture().join();

        // Print out any failed uploads
        completedDirectoryUpload.failedTransfers().forEach(System.out::println);

    }

    @AfterEach
    void afterAll() {
        // clear postgres database
        System.out.println("--- SOME TEST TEARDOWN ---");
        this.jdbcTemplate.execute("DROP SCHEMA IF EXISTS metadata CASCADE;");

        // clear s3 buckets
        TestUtils.tearDownS3(); // TODO: implement
    }

    @Test
    public void testJob(@Autowired Job job) throws Exception {
        this.jobLauncherTestUtils.setJob(job);
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
    }
}
