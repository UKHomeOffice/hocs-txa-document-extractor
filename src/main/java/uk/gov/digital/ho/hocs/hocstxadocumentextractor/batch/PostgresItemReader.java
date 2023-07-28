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
    private static final Logger log = LoggerFactory.getLogger(PostgresItemReader.class);
    public String lastSuccessfulCollection;
    public DataSource dataSource;
    public String metadataSchema;
    public String metadataTable;
    public int fetchSize;
    public boolean deletes;

    public PostgresItemReader(final DataSource dataSource,
                              final String metadataSchema,
                              final String metadataTable,
                              final int fetchSize,
                              final boolean deletes,
                              final String hocsSystem) {
        log.info("Constructing PostgresItemReader");
        this.dataSource = dataSource;
        this.metadataSchema = metadataSchema;
        this.metadataTable = metadataTable;
        this.fetchSize = fetchSize;
        this.deletes = deletes;

        String temp_query = "";
        setDataSource(this.dataSource);
        setFetchSize(this.fetchSize);
        setName("documentReader");
        setSql(temp_query);
        setRowMapper(new DocumentRowMapper(hocsSystem));
    }

    @BeforeStep
    public void setSqlParams(StepExecution stepExecution) {
        /*
        This selects the correct SQL query for the given mode (deletes or ingest)
        and modifies the SQL statement of the DataSource to the correct query, replacing
        placeholders with actual values.
         */
        log.info("Inserting values into ItemReader SQL statement...");
        log.info("Checking JobExecutionContext for lastSuccessfulCollection timestamp");
        JobExecution jobExecution = stepExecution.getJobExecution();
        ExecutionContext jobContext = jobExecution.getExecutionContext();
        String timestamp = jobContext.getString("lastSuccessfulCollection");
        log.info("Found timestamp: " + timestamp);
        this.lastSuccessfulCollection = timestamp;

        String templateSQL;
        if (this.deletes) {
            log.info("Application is in DELETE mode. It will query documents to collect for deletion.");
            templateSQL = getDeleteQuery();
        } else {
            log.info("Application is in INGEST mode. It will query documents to collect for ingestion.");
            templateSQL = getIngestQuery();
        }

        log.info("Updating SQL with parameters:");
        log.info("metadataSchema=" + this.metadataSchema);
        log.info("metadataTable=" + this.metadataTable);
        log.info("lastSuccessfulCollection=" + this.lastSuccessfulCollection);
        String actualSQL = templateSQL
            .replace("$schema", this.metadataSchema)
            .replace("$table", this.metadataTable)
            .replace("$timestamp", this.lastSuccessfulCollection);

        setSql(actualSQL);
    }

    public String getIngestQuery() {
        /*
        Returns the query for collecting documents to INGEST into the text analytics pipeline
         */
        String ingestQuery = """
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
                    AND pdf_link IS NOT NULL
                    AND deleted_on IS NULL
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
        return ingestQuery;
    }

    public String getDeleteQuery() {
        /*
        Returns the query for collecting documents to DELETE from the text analytics pipeline.

        This query looks for all newly deleted relevant documents since the last collection of deletes
        plus a window of 1 week. This additional 1 week is meant to ensure all deletes are fully
        propagated downstream and deal with the edge case where a delete message is processed by the
        downstream application before an ingest message is fully processed.
         */
        String deleteQuery = """
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
                    AND pdf_link IS NOT NULL
                    AND deleted_on > '$timestamp'::timestamp - interval '1 week'
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
        return deleteQuery;
    }

}
