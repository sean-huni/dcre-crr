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

    @Bean
    public Job crrJob(JobRepository repo, PlatformTransactionManager tx,
                      HeaderTasklet headerTasklet, Step detailStep, CrrJobListener listener) {
        Step headerStep = new StepBuilder("headerStep", repo)
                .tasklet(headerTasklet, tx)
                .build();
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
