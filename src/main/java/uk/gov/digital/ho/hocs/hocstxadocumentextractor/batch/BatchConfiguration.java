package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentRow;

import javax.sql.DataSource;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class BatchConfiguration {

    private @Value("${document-metadata.metadata_schema}") String metadataSchema;
    private @Value("${document-metadata.metadata_table}") String metadataTable;
    private @Value("${document-metadata.fetch_size}") Integer fetchSize;
    private @Value("${document-metadata.chunk_size}") Integer chunkSize;
    private @Value("${document-metadata.last_ingest}") String lastIngest;
    private @Value("${document-metadata.last_delete}") String lastDelete;
    private @Value("${s3.source_bucket}") String sourceBucket;
    private @Value("${s3.target_bucket}") String targetBucket;
    private @Value("${s3.endpoint_url}") String endpointURL;
    private @Value("${slack.decs_channel}") String decsSlackURL;
    private @Value("${slack.txa_channel}") String txaSlackURL;
    private @Value("${mode.delete}") boolean deletes;

    @Bean
    public SlackNotification slackNotification() {
        Map<String, String> slackURLMap = new HashMap<String, String>();
        slackURLMap.put("txa", txaSlackURL);
        slackURLMap.put("decs", decsSlackURL);
        return new SlackNotification(slackURLMap, deletes);
    }

    @Bean
    public JobStartFinishListener jobListener(SlackNotification slackNotification) throws URISyntaxException {
        return new JobStartFinishListener(targetBucket,
            endpointURL,
            lastIngest,
            lastDelete,
            deletes,
            slackNotification);
    }

    @Bean
    public ExecutionContextPromotionListener promotionListener() {
        /*
        Takes care of 'promoting' StepExecutionContext information to the JobExecutionContext.
        Makes information recorded at the step level available to other elements of the Job.
         */
        ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[] {"lastSuccessfulCollection", "readCount"});
        listener.setStatuses(new String[] {"*"});  // promote keys regardless of step outcome.
        return listener;
    }

    @Bean
    public PostgresItemReader reader(@Qualifier("metadataSource") DataSource metadataSource) {
        return new PostgresItemReader(metadataSource,
            metadataSchema,
            metadataTable,
            fetchSize,
            deletes);
    }

    @Bean
    public S3ItemProcessor processor() throws URISyntaxException {
        return new S3ItemProcessor(sourceBucket, targetBucket, endpointURL);
    }

    @Bean
    public TxaKafkaItemWriter writer(KafkaTemplate kafkaTemplate) throws Exception {
        return new TxaKafkaItemWriter(targetBucket, endpointURL, txaSlackURL, decsSlackURL, deletes, kafkaTemplate);
    }

    @Bean
    public Step mainStep(JobRepository jobRepository,
                         PlatformTransactionManager transactionManager,
                         PostgresItemReader reader,
                         S3ItemProcessor processor,
                         TxaKafkaItemWriter writer,
                         ReadCountStepExecutionListener listener,
                         ExecutionContextPromotionListener promotionListener) {
        if (deletes) {
            /*
            An identical step definition to ingest (not delete) mode except that
            the processor (which copies documents between S3 buckets) is omitted.
             */
            return new StepBuilder("mainStep", jobRepository)
                .startLimit(1)
                .<DocumentRow, DocumentRow> chunk(chunkSize, transactionManager)
                .reader(reader)
                .writer(writer)
                .listener(promotionListener)  // Must be declared first so its afterStep runs after the ReadCountStepExecutionListener
                .listener(listener)
                .build();
        }
        return new StepBuilder("mainStep", jobRepository)
                .startLimit(1)
                .<DocumentRow, DocumentRow> chunk(chunkSize, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .listener(promotionListener)  // Must be declared first so its afterStep runs after the ReadCountStepExecutionListener
                .listener(listener)
                .build();
    }

    @Bean
    public Job documentExtractionJob(JobRepository jobRepository,
                                     JobStartFinishListener listener,
                                     Step mainStep) {
        return new JobBuilder("documentExtractionJob", jobRepository)
            .incrementer(new RunIdIncrementer())
            .listener(listener)
            .flow(mainStep)
            .end()
            .build();
    }

}
