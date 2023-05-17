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
            WITH all_case_document_types AS (
                SELECT
                    uuid::text,
                    external_reference_uuid::text,
                    RIGHT(external_reference_uuid::text, 2) as case_type,
                    type,
                    pdf_link,
                    status,
                    updated_on,
                    deleted_on
                FROM
                    $schema.$table
                WHERE
                    status in ('UPLOADED')
                    AND pdf_link is NOT NULL
                    AND updated_on > '$timestamp'::timestamp
            )
            SELECT
                uuid,
                external_reference_uuid,
                case_type,
                type,
                pdf_link,
                status,
                updated_on,
                deleted_on
            FROM
                all_case_document_types
            WHERE
                (case_type = 'a1' AND type in ('ORIGINAL', 'CONTRIBUTION'))
                OR (case_type = 'a2' AND type in ('ORIGINAL', 'CONTRIBUTION'))
                OR (case_type = 'a3' AND type in ('ORIGINAL', 'CONTRIBUTION'))
                OR (case_type = 'a4' AND type in ('Original Complaint', 'Contribution Response'))
                OR (case_type = 'a5' AND type in ('Original Complaint', 'Contribution Response'))
                OR (case_type = 'b5' AND type in ('Original correspondence', 'Further correspondence from MPs Office', 'Contributions received'))
                OR (case_type = 'b6' AND type in ('Original correspondence', 'Further correspondence from MPs Office', 'Contributions received'))
                OR (case_type = 'c1' AND type in ('Claim form', 'Supporting evidence'))
                OR (case_type = 'c5' AND type in ('To document', 'Public correspondence', 'Complaint leaflet', 'Complaint letter', 'Email', 'CRF', 'Appeal Leaflet', 'IMB Letter'))
                OR (case_type = 'c6' AND type in ('To document', 'Public correspondence', 'Complaint leaflet', 'Complaint letter', 'Email', 'CRF', 'Appeal Leaflet', 'IMB Letter'))
                OR (case_type = 'c7' AND type in ('Original complaint'))
                OR (case_type = 'c9' AND type in ('To document', 'Public correspondence', 'Complaint leaflet', 'Complaint letter', 'Email', 'CRF'))
                OR (case_type = 'd1' AND type in ('Initial Correspondence', 'Contribution Response'))
                OR (case_type = 'e1' AND type in ('To document', 'Public correspondence', 'Complaint leaflet', 'Complaint letter', 'Email', 'CRF'))
            ORDER BY updated_on ASC;
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
