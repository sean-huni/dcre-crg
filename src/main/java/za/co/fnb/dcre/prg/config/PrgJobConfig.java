package za.co.fnb.dcre.prg.config;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.boot.ApplicationRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.transaction.PlatformTransactionManager;
import za.co.fnb.dcre.platform.batch.OutcomeFileWriter;
import za.co.fnb.dcre.platform.batch.StaleExecutionSweeper;
import za.co.fnb.dcre.prg.service.PsrTasklet;

import javax.sql.DataSource;
import java.nio.file.Path;

@Configuration
public class PrgJobConfig {

    @Bean
    public Job prgJob(JobRepository repo, PlatformTransactionManager tx, PsrTasklet tasklet,
                      @Value("${dcre.exchange-root}") String exchangeRoot) {
        Step psrStep = new StepBuilder("psrStep", repo).tasklet(tasklet, tx).build();
        return new JobBuilder("prgJob", repo)
                .listener(new SeamListener(exchangeRoot))
                .start(psrStep)
                .build();
    }

    @Bean
    @Order(-10)
    public ApplicationRunner staleExecutionSweep(DataSource dataSource) {
        return args -> StaleExecutionSweeper.abandonStale(dataSource, "PRG_BATCH_", 60);
    }

    record SeamListener(String exchangeRoot) implements JobExecutionListener {

        @Override
        public void afterJob(JobExecution execution) {
            if (execution.getStatus() != BatchStatus.COMPLETED) {
                return;
            }
            String jobName = System.getenv().getOrDefault("JOB_NAME", "local-" + execution.getId());
            OutcomeFileWriter.write(Path.of(exchangeRoot), jobName, "BUSINESS_ACCEPTED");
        }
    }
}
