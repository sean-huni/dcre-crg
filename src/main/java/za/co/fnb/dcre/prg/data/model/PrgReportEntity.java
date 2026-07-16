package za.co.fnb.dcre.prg.data.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * SCRUM-55 report registry: one row per emitted report artifact
 * (report_type SCHEDULED | IMMEDIATE | HEARTBEAT | MANUAL). Plain aggregate,
 * NOT a BaseEntity: prg_report is append-only (no version/updated_at
 * columns), so new-ness rides {@link Persistable} instead of the @Version
 * heuristic and the factory assigns id + created_at client-side.
 */
@Table("prg_report")
public class PrgReportEntity implements Persistable<UUID> {

    @Id
    private UUID id;
    private String client;
    private String reportType;
    private String triggerKind;
    private String windowKey;
    private String parentSourceMsgId;
    private String fileName;
    private Instant createdAt;

    @Transient
    private boolean isNew;

    public static PrgReportEntity of(final String client, final String reportType,
            final String triggerKind, final String windowKey, final String parentSourceMsgId,
            final String fileName) {
        PrgReportEntity r = new PrgReportEntity();
        r.id = UUID.randomUUID();
        r.isNew = true;
        r.client = client;
        r.reportType = reportType;
        r.triggerKind = triggerKind;
        r.windowKey = windowKey;
        r.parentSourceMsgId = parentSourceMsgId;
        r.fileName = fileName;
        r.createdAt = Instant.now();
        return r;
    }

    @Override
    public UUID getId() { return id; }

    @Override
    public boolean isNew() { return isNew; }

    public String getClient() { return client; }
    public String getReportType() { return reportType; }
    public String getTriggerKind() { return triggerKind; }
    public String getWindowKey() { return windowKey; }
    public String getParentSourceMsgId() { return parentSourceMsgId; }
    public String getFileName() { return fileName; }
    public Instant getCreatedAt() { return createdAt; }
}
