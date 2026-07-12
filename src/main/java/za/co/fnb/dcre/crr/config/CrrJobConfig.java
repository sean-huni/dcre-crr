package za.co.fnb.dcre.crr.config;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import za.co.fnb.dcre.crr.data.repo.TxEntryRepo;
import org.springframework.transaction.PlatformTransactionManager;
import za.co.fnb.dcre.crr.service.CrrJobListener;
import za.co.fnb.dcre.crr.service.HeaderTasklet;
import za.co.fnb.dcre.crr.service.SpineWriter;

import java.util.UUID;

/**
 * CRR job: headerStep (tasklet: parse + persist tx_header, structural checks,
 * FILE_FATAL routing) then detailStep (chunked line -> tx_entry upsert,
 * restartable per R-05). Identifying JobParameter: arrival.id (R-16).
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
    public Step detailStep(JobRepository repo, PlatformTransactionManager tx,
                           FlatFileItemReader<String> detailReader, SpineWriter writer) {
        return new StepBuilder("detailStep", repo)
                .<String, String>chunk(100, tx)
                .reader(detailReader)
                .writer(chunk -> writer.writeDetails(chunk.getItems()))
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<String> detailReader(@Value("#{jobParameters['input.file']}") String inputFile) {
        return new FlatFileItemReaderBuilder<String>()
                .name("detailReader")
                .resource(new FileSystemResource(inputFile))
                .linesToSkip(1) // header handled by headerStep
                .lineMapper((line, lineNumber) -> line)
                .build();
    }

    @Bean
    @StepScope
    public SpineWriter spineWriter(TxEntryRepo repo,
                                   @Value("#{jobParameters['arrival.id']}") String arrivalId,
                                   @Value("${dcre.amount-scale}") int amountScale,
                                   @Value("${dcre.v1-enabled}") boolean v1Enabled) {
        return new SpineWriter(repo, UUID.fromString(arrivalId), amountScale, v1Enabled);
    }
}
