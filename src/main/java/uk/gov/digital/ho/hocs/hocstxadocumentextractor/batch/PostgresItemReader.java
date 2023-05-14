package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentRow;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentRowMapper;

import javax.sql.DataSource;

public class PostgresItemReader extends JdbcCursorItemReader<DocumentRow> {
    /*
    This class defines the rules (SQL query) to determine what documents to
    collect when the Job executes.
     */
    private static final Logger log = LoggerFactory.getLogger(
        PostgresItemReader.class);
    public String lastSuccessfulCollection;
    public DataSource dataSource;
    public String metadataSchema;
    public String metadataTable;
    public int fetchSize;

    public PostgresItemReader(final DataSource dataSource,
                              final String metadataSchema,
                              final String metadataTable,
                              final int fetchSize) {
        log.info("Constructing PostgresItemReader");
        this.dataSource = dataSource;
        this.metadataSchema = metadataSchema;
        this.metadataTable = metadataTable;
        this.fetchSize = fetchSize;

        String document_query = """
            SELECT
                document_id,
                uploaded_date,
                relevant_document,
                s3_key
            FROM
                $schema.$table
            WHERE
                relevant_document = 'Y'
                AND uploaded_date > '$timestamp'::timestamp
            ORDER BY uploaded_date ASC;
            """;

        setDataSource(this.dataSource);
        setFetchSize(this.fetchSize);
        setName("documentReader");
        setSql(document_query);
        setRowMapper(new DocumentRowMapper());
    }

    @BeforeStep
    public void getSqlParams(StepExecution stepExecution) {
        /*
        This modifies the SQL statement of the DataSource to replace placeholders
        with actual values.
         */
        log.info("Inserting values into ItemReader SQL statement...");
        log.info("Checking JobExecutionContext for lastSuccessfulCollection timestamp");
        JobExecution jobExecution = stepExecution.getJobExecution();
        ExecutionContext jobContext = jobExecution.getExecutionContext();
        String timestamp = jobContext.getString("lastSuccessfulCollection");
        log.info("Found: " + timestamp);
        this.lastSuccessfulCollection = timestamp;
        log.info("Updating ItemReader SQL with:");
        log.info("metadataSchema=" + this.metadataSchema);
        log.info("metadataTable=" + this.metadataTable);
        log.info("lastSuccessfulCollection=" + this.lastSuccessfulCollection);
        String template_sql = getSql();
        String actual_sql = template_sql
            .replace("$schema", this.metadataSchema)
            .replace("$table", this.metadataTable)
            .replace("$timestamp", this.lastSuccessfulCollection);

        setSql(actual_sql);
    }

}
