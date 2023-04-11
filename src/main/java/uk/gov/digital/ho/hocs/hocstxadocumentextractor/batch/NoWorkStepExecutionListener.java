package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

@Component
public class NoWorkStepExecutionListener implements StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(NoWorkStepExecutionListener.class);

    public void beforeStep(StepExecution stepExecution) {
        log.info("Records processed by this step will be counted");
    }

    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("Executing afterStep tasks...");
        long readCount = stepExecution.getReadCount();
        if (readCount == 0) {
            log.warn("Processed " + readCount + " records");
            log.warn("This may or may not be unexpected");
        } else {
            log.info("Processed " + readCount + " records");
        }
        // Write the readCount to the step context so it can be promoted to the job context
        // and reported on in the afterJob listener.
        ExecutionContext stepContext = stepExecution.getExecutionContext();
        stepContext.putLong("readCount", readCount);
        return null;
    }

}
