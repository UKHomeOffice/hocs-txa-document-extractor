package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class NoWorkStepExecutionListener implements StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(NoWorkStepExecutionListener.class);

    public void beforeStep(StepExecution stepExecution) {
        log.info("Documents processed will be counted");
    }

    public ExitStatus afterStep(StepExecution stepExecution) {
        long readCount = stepExecution.getReadCount();
        if (readCount == 0) {
            log.warn("Processed " + readCount + " documents");
            return null;
        }
        log.info("Processed " + readCount + " documents");
        return null;
    }

}
