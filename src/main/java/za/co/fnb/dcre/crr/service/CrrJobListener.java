package za.co.fnb.dcre.crr.service;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import za.co.fnb.dcre.platform.batch.OutcomeFileWriter;

import java.nio.file.Path;

/**
 * Writes the business-verdict seam (SYNTHETIC-CONTRACT, R-35): ACCEPTED on a
 * clean run, FILE_FATAL on a structural verdict. Technical failure writes
 * nothing: the exit code and the K8s Failed condition are the witnesses
 * (R-33 arbiter clause).
 */
@Component
public class CrrJobListener implements JobExecutionListener {

    private final String exchangeRoot;

    public CrrJobListener(@Value("${dcre.exchange-root}") String exchangeRoot) {
        this.exchangeRoot = exchangeRoot;
    }

    @Override
    public void afterJob(JobExecution execution) {
        String jobName = System.getenv().getOrDefault("JOB_NAME", "local-" + execution.getId());
        if (execution.getStatus() != BatchStatus.COMPLETED) {
            return;
        }
        String verdict = execution.getExecutionContext().containsKey("fileFatalReason")
                ? "BUSINESS_FILE_FATAL" : "BUSINESS_ACCEPTED";
        OutcomeFileWriter.write(Path.of(exchangeRoot), jobName, verdict);
    }
}
