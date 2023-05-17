package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.kafka.core.KafkaTemplate;
import uk.gov.digital.ho.hocs.hocstxadocumentextractor.documents.DocumentRow;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class TxaKafkaItemWriter extends KafkaItemWriter<String, DocumentRow> {
    /*
    Spring Batch ItemWriter class to write DECS documents metadata to Text Analytics
    Kafka cluster for ingestion.
     */
    private static final Logger log = LoggerFactory.getLogger(
        TxaKafkaItemWriter.class);
    private StepExecution stepExecution;
    private final String targetBucket;
    private final String endpointURL;
    private final String txaSlackURL;
    private final String decsSlackURL;
    private final boolean deletes;
    private final KafkaTemplate kafkaTemplate;

    TxaKafkaItemWriter(String targetBucket, String endpointURL, String txaSlackURL, String decsSlackURL, boolean deletes, KafkaTemplate kafkaTemplate) throws Exception {
        this.targetBucket = targetBucket;
        this.endpointURL = endpointURL;
        this.txaSlackURL = txaSlackURL;
        this.decsSlackURL = decsSlackURL;
        this.deletes = deletes;
        this.kafkaTemplate = kafkaTemplate;
        setKafkaTemplate(kafkaTemplate);
        setItemKeyMapper(DocumentRow::getUuid);
        setDelete(false); // not related to the HocsTxaDocumentExtractor delete functionality
        setTimeout(10000); // Milliseconds to wait for callback
        afterPropertiesSet();
    }

    @Override
    public void write(Chunk<? extends DocumentRow> doc_list) throws Exception {
        if (doc_list == null) {
            return;
        }
        String checkpointTimestamp = null;
        for (DocumentRow doc : doc_list) {
            log.info("Publishing event for doc=" + doc.getUuid() + " with timestamp=" + doc.getUpdatedOn().toString());
            String key = itemKeyMapper.convert(doc);
            writeKeyValue(key, doc);

            checkpointTimestamp = doc.getUpdatedOn().toString();
        }
        /*
        Only update the checkpoint timestamp if the write is definitely successful.
        This is us committing the progress of the job.

        flush() should throw an exception if there is an error with the delivery of a
        message. flush() calls flush on the underlying producer before checking all
        completableFutures to confirm the broker received the messages.
         */
        flush();

        log.info("Updating checkpointTimestamp in StepContext with " + checkpointTimestamp);
        ExecutionContext stepContext = this.stepExecution.getExecutionContext();
        stepContext.putString("lastSuccessfulCollection", checkpointTimestamp);
    }

    @BeforeStep
    public void saveStepExecution(StepExecution stepExecution) {
        /*
        Makes the step Execution available to the write method so it can be updated
        as chunks are processed.
         */
        this.stepExecution = stepExecution;
    }

    @PreDestroy
    public void commitTimestamp() {
        /*
        The purpose of this method is to commit the timestamp of the last successful document sent
        to the text analytics pipeline. This is defined here (instead of the JobStartFinishListener)
        to ensure that it is always invoked - whether the job finishes successfully or the job is
        interrupted (for example by a Kubernetes SIGTERM signal).

        I think this is necessary to occur here because on an unexpected interruption e.g. SIGTERM,
        other beans such as the JobStartFinishListener will start getting destroyed immediately.
        The PromotionListener (to move the timestamp from step to job) will not run.
        This class is the one place where an accurate last successful timestamp can be retrieved to attempt
        to commit it before the application exits.

        This method creates new class instances (rather than using Beans created by Spring) because
        pre-existing beans will already be destroyed before this class can utilise them.
         */
        String lastCheckpointTimestamp = this.stepExecution.getExecutionContext().getString("lastSuccessfulCollection", "empty");
        if (lastCheckpointTimestamp == "empty") {
            log.info("Timestamp is null in ExecutionContext so committing it is skipped.");
            return;
        }

        try{
            log.info("Trying to commit the last successful timestamp...");
            S3TimestampManager timestampManager = new S3TimestampManager(this.targetBucket,
                this.endpointURL,
                "",
                "",
                this.deletes);

            timestampManager.getTimestamp();
            boolean success = timestampManager.putTimestamp(lastCheckpointTimestamp);

            Map<String, String> slackURLMap = new HashMap<String, String>();
            slackURLMap.put("txa", this.txaSlackURL);
            slackURLMap.put("decs", this.decsSlackURL);
            SlackNotification slackNotification = new  SlackNotification(slackURLMap, this.deletes);
            String timestampMessage = slackNotification.craftTimestampMessage(success, lastCheckpointTimestamp);

            if (success) {
                log.info("Timestamp committed successfully during PreDestroy");
                slackNotification.publishMessage(timestampMessage, "txa");
            }
            else {
                log.error("committing the checkpointTimestamp failed");
                log.error("the next execution of the job will reprocesses uncommitted records");
                slackNotification.publishMessage(timestampMessage, "txa");
                slackNotification.publishMessage(timestampMessage, "decs");
            }
        } catch (URISyntaxException | IOException e) {
            log.error(e.toString());
        }
    }
}
