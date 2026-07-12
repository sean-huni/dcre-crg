package za.co.fnb.dcre.prg.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import za.co.fnb.dcre.platform.files.StagedWrite;
import za.co.fnb.dcre.prg.data.model.StatusRow;
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

    private final PrgWatermarkRepo watermarks;
    private final String exchangeRoot;

    public PsrReportService(PrgWatermarkRepo watermarks,
                            @Value("${dcre.exchange-root}") String exchangeRoot) {
        this.watermarks = watermarks;
        this.exchangeRoot = exchangeRoot;
    }

    /** @return the emitted PSR file path, or empty when the window carries no delta. */
    public Optional<Path> window(String client, String windowKey, boolean resend) throws IOException {
        List<StatusRow> rows = resend ? watermarks.findRange(client) : watermarks.findDelta(client);
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        Path target = Path.of(exchangeRoot, "onhost-resp", client + "_PSR_" + windowKey + ".txt");
        StagedWrite.write(target, render(client, windowKey, rows));
        for (StatusRow row : rows) {
            watermarks.upsertWatermark(client, row.e2e(), row.status());
        }
        return Optional.of(target);
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
