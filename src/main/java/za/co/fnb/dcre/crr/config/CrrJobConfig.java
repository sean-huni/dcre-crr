package za.co.fnb.dcre.crr.config;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import za.co.fnb.dcre.crr.batch.FixedRecordRangeReader;
import za.co.fnb.dcre.crr.batch.LineRangePartitioner;
import za.co.fnb.dcre.crr.data.repo.TxEntryBatchDao;
import org.springframework.transaction.PlatformTransactionManager;
import za.co.fnb.dcre.crr.service.CrrJobListener;
import za.co.fnb.dcre.crr.service.HeaderTasklet;
import za.co.fnb.dcre.crr.service.SpineWriter;
import za.co.fnb.dcre.platform.batch.CrdbRetryExceptionHandler;
import za.co.fnb.dcre.platform.batch.PartitionSizer;

import java.util.UUID;

/**
 * CRR job: headerStep (tasklet: parse + persist tx_header, structural checks,
 * FILE_FATAL routing) then detailStep, a partitioned manager/worker pair
 * (R-41): byte-offset record ranges fan out onto virtual threads, each worker
 * a chunked range read -> batched tx_entry upsert, restartable per R-05.
 * Identifying JobParameter: arrival.id (R-16).
 */
@Configuration
public class CrrJobConfig {

    /**
     * CRDB 40001 retry for the tasklet WRITE step (headerStep: tx_header
     * persist), same failure class CDE hit live under multi-copybook load.
     * Commit-time aborts are covered: the tasklet commit runs inside the
     * step's repeat loop and the re-run redoes the whole tasklet in a fresh
     * transaction (shared platform handler, proven live in CTV).
     *
     * <p>Deliberately NOT registered on the chunk-oriented detailWorkerStep:
     * ChunkOrientedTasklet removes its buffered INPUTS and marks the chunk
     * context complete BEFORE the transaction commits, and
     * StepContextRepeatCallback only re-queues INCOMPLETE chunk contexts
     * (spring-batch-core 6.0.4), so a swallowed commit-time abort there would
     * silently DROP the in-flight chunk (verified empirically 2026-07-14:
     * step COMPLETED with writes=[[0,1],[2]] after chunk [0,1] aborted at
     * commit). A commit abort on the worker stays step-FAILED -> relaunch,
     * which re-reads from read.count and re-writes via the guarded upsert
     * keyed (arrival_id, sequence) (R-05).
     */
    private final CrdbRetryExceptionHandler crdbRetry = new CrdbRetryExceptionHandler("CRR");

    @Bean
    public Step headerStep(JobRepository repo, PlatformTransactionManager tx, HeaderTasklet headerTasklet) {
        return new StepBuilder("headerStep", repo)
                .tasklet(headerTasklet, tx)
                .exceptionHandler(crdbRetry)
                .build();
    }

    @Bean
    public Job crrJob(JobRepository repo, Step headerStep, Step detailStep, CrrJobListener listener) {
        return new JobBuilder("crrJob", repo)
                .listener(listener)
                .start(headerStep)
                    .on(HeaderTasklet.EXIT_FILE_FATAL).end() // business verdict, job COMPLETED
                .from(headerStep).on("*").to(detailStep)
                .end()
                .build();
    }

    @Bean
    public Step detailStep(JobRepository repo, Step detailWorkerStep,
                           LineRangePartitioner partitioner,
                           @Value("${dcre.crr.max-partitions:5}") int maxPartitions) {
        return new StepBuilder("detailStep", repo)
                .partitioner("detailWorkerStep", partitioner)
                .step(detailWorkerStep)
                .gridSize(PartitionSizer.partitions(maxPartitions))
                .taskExecutor(new VirtualThreadTaskExecutor("crr-part-"))
                .build();
    }

    @Bean
    public Step detailWorkerStep(JobRepository repo, PlatformTransactionManager tx,
                                 FixedRecordRangeReader rangeReader, SpineWriter writer) {
        return new StepBuilder("detailWorkerStep", repo)
                .<SpineWriter.NumberedLine, SpineWriter.NumberedLine>chunk(100, tx)
                .reader(rangeReader)
                .writer(chunk -> writer.writeDetails(chunk.getItems()))
                .build();
    }

    @Bean
    @StepScope
    public SpineWriter spineWriter(TxEntryBatchDao dao,
                                   @Value("#{jobParameters['arrival.id']}") String arrivalId,
                                   @Value("${dcre.amount-scale}") int amountScale,
                                   @Value("${dcre.v1-enabled}") boolean v1Enabled) {
        return new SpineWriter(dao, UUID.fromString(arrivalId), amountScale, v1Enabled);
    }
}
