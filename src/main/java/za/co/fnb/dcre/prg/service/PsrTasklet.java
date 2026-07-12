package za.co.fnb.dcre.prg.service;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

/**
 * Thin entry adapter (3-tier). Job identity: (client, window); resend is a
 * non-identifying override ("true") that re-emits all current rows.
 */
@Component
public class PsrTasklet implements Tasklet {

    private final PsrReportService service;

    public PsrTasklet(PsrReportService service) {
        this.service = service;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        var params = chunkContext.getStepContext().getJobParameters();
        String client = (String) params.get("client");
        String window = (String) params.get("window");
        boolean resend = "true".equals(params.get("resend"));
        var emitted = service.window(client, window, resend);
        chunkContext.getStepContext().getStepExecution().getJobExecution()
                .getExecutionContext().putString("psr.file",
                        emitted.map(Object::toString).orElse("NONE"));
        return RepeatStatus.FINISHED;
    }
}
