package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentRow;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentRowMapper;

import javax.sql.DataSource;

@Configuration
public class BatchConfiguration {

    @Bean
    public JdbcCursorItemReader<DocumentRow> reader(@Qualifier("metadataSource") DataSource metadataSource) {
        /*
        Read rows from the metadataSource.
        TODO: parametrise from config (fetchSize, schema/table)
         */
        String document_query = """
            SELECT
                document_id,
                uploaded_date,
                relevant_document,
                s3_key
            FROM
                metadata.document_metadata
            WHERE
                relevant_document = 'Y'
                AND uploaded_date >= '2023-03-22 00:00:00'::timestamp
            """;

        return new JdbcCursorItemReaderBuilder<DocumentRow>()
            .dataSource(metadataSource)
            .fetchSize(2)
            .name("documentReader")
            .sql(document_query)
            .rowMapper(new DocumentRowMapper())
            .build();
    }

    @Bean
    public DocumentItemProcessor processor() {
        return new DocumentItemProcessor();
    }

    @Bean
    public DocumentItemWriter writer() {
        return new DocumentItemWriter();
    }

    @Bean
    public Job documentExtractionJob(JobRepository jobRepository,
                             JobCompletionNotificationListener listener,
                             Step mainStep) {
        return new JobBuilder("documentExtractionJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(mainStep)
                .end()
                .build();
    }

    @Bean
    public Step mainStep(JobRepository jobRepository,
                         PlatformTransactionManager transactionManager,
                         JdbcCursorItemReader<DocumentRow> reader,
                         DocumentItemProcessor processor,
                         DocumentItemWriter writer) {
        return new StepBuilder("mainStep", jobRepository)
                .startLimit(1)
                .<DocumentRow, DocumentRow> chunk(2, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

}
