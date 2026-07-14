package za.co.fnb.dcre.prg.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import za.co.fnb.dcre.platform.files.ExchangeChannel;
import za.co.fnb.dcre.platform.files.ExchangeLayout;
import za.co.fnb.dcre.platform.files.ExchangeSub;
import za.co.fnb.dcre.platform.files.StagedWrite;
import za.co.fnb.dcre.prg.data.model.StatusRow;
import za.co.fnb.dcre.prg.data.model.UnknownRow;
import za.co.fnb.dcre.prg.data.repo.PrgWatermarkRepo;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Business tier: per-client delta PSR emission on clock windows.
 * PSR flat file layout is a SYNTHETIC-CONTRACT (R-35): header
 * "PSR|client|window", one "TX|e2e|status" per row, trailer "END|count".
 * Zero delta rows = NO file for the window; resend re-emits ALL current
 * rows. R-29 order: file first (StagedWrite), watermark advance second,
 * so a crash between the two replays as a StagedWrite no-op + advance.
 */
@Service
public class PsrReportService {

    private static final Logger log = LoggerFactory.getLogger(PsrReportService.class);

    private final PrgWatermarkRepo watermarks;
    private final ExchangeLayout layout;
    private final TransactionTemplate watermarkTx;

    public PsrReportService(final PrgWatermarkRepo watermarks, final ExchangeLayout layout,
                            final PlatformTransactionManager txManager) {
        this.watermarks = watermarks;
        this.layout = layout;
        // Each watermark-advance attempt needs its OWN transaction: a CRDB
        // 40001 abort poisons the surrounding transaction (25P02 on any further
        // statement), so retrying inside the step transaction can never succeed.
        this.watermarkTx = new TransactionTemplate(txManager);
        this.watermarkTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    /** @return the emitted PSR file path, or empty when the window carries no delta. */
    public Optional<Path> window(String client, String windowKey, boolean resend) throws IOException {
        // R-38 exclusion visibility: mid-DAG rows (status NULL) never reach a PSR.
        for (UnknownRow unknown : watermarks.findUnknown(client)) {
            log.warn("excluded stage=PRG arrival={} seq={} e2e={} reason=STATUS_UNKNOWN",
                    unknown.arrivalId(), unknown.sequence(), unknown.e2e());
        }
        List<StatusRow> rows = resend ? watermarks.findRange(client) : watermarks.findDelta(client);
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        Path target = layout.resolve(client, ExchangeChannel.ONHOST_RESP, ExchangeSub.OUT)
                .resolve(client + "_PSR_" + windowKey + ".txt");
        StagedWrite.write(target, render(client, windowKey, rows));
        // R-29: file exists by now, so replaying the advance is safe (upsert
        // keyed (client, e2e)); WriteTooOldError under load is a routine 40001.
        advanceWatermarks(client, rows);
        return Optional.of(target);
    }

    private void advanceWatermarks(final String client, final List<StatusRow> rows) {
        CrdbRetry.run("watermark-advance client=" + client, () ->
                watermarkTx.executeWithoutResult(status -> {
                    for (StatusRow row : rows) {
                        watermarks.upsertWatermark(client, row.e2e(), row.status());
                    }
                }));
    }

    private List<String> render(String client, String windowKey, List<StatusRow> rows) {
        List<String> lines = new ArrayList<>();
        lines.add("PSR|" + client + "|" + windowKey);
        for (StatusRow row : rows) {
            lines.add("TX|" + row.e2e() + "|" + row.status());
        }
        lines.add("END|" + rows.size());
        return lines;
    }
}
