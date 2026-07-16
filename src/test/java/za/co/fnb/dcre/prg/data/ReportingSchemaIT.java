package za.co.fnb.dcre.prg.data;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;
import za.co.fnb.dcre.prg.data.model.LedgerRow;
import za.co.fnb.dcre.prg.data.model.PrgReportEntity;
import za.co.fnb.dcre.prg.data.repo.PrgDeliveryLedgerRepo;
import za.co.fnb.dcre.prg.data.repo.PrgReportRepo;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * SCRUM-55 Task 9 schema contract (the plan's deliberately-unfrozen
 * prg_report_due SQL is DEFINED by these cases):
 * (a) ext_tx_status v2 is batch-scoped: an identical e2e under a different
 *     arrival no longer cross-links a response (A-40 guard); a legacy
 *     response with emission_id NULL still projects (fail-open ingest truth).
 * (b) prg_delivery_ledger auto rows are DB-arbitrated once per
 *     (client, e2e, status); manual_ref rows bypass the guard but ledger.
 * (c) prg_report_due lists COMPLETE (all members terminal, immediately) and
 *     IDLE (responses quiet past the 120s debounce with unreported deltas);
 *     recent, fully-ledgered and zero-response parents are absent.
 * (d) an unclassified status code (ACWC) fails closed as interim.
 * (e) prg_sla_pending ages non-terminal members of VISIBLE batches.
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class ReportingSchemaIT {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
    }

    static final LocalDate RUN = LocalDate.of(2026, 7, 16);

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    PrgReportRepo reports;

    @Autowired
    PrgDeliveryLedgerRepo ledger;

    record ExtRow(String status, boolean terminal, UUID emissionId, String outboundMsgId, String sourceMsgId) {
    }

    // --- seed helpers (tables exist via the 001/003 bootstrap guards) ---

    UUID parent(String client, String msgId) {
        UUID arrival = UUID.randomUUID();
        jdbc.update("INSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,"
                        + " initg_pty, business_date, client_token, layout_version) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, msgId, msgId, "20260716080000", 1, client, "20260716", client, 2);
        return arrival;
    }

    void tx(UUID arrival, int seq, String e2e) {
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,"
                        + " creditor_account, currency, amount_raw, amount) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, seq, "DC", e2e, e2e, "62000000010", "ZAR", "1000", 10.00);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,'PASS')", arrival, seq);
    }

    UUID group(UUID arrival, String client, String msgId, int batches) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO crw_emission_group (id, arrival_id, client, source_msg_id, run_date,"
                        + " applied_max, total_tx, total_amount, expected_batch_count, split)"
                        + " VALUES (?,?,?,?,?,?,?,?,?,?)",
                id, arrival, client, msgId, RUN, 5000, 2L, 20.00, batches, batches > 1);
        return id;
    }

    UUID batch(UUID groupId, UUID arrival, int ordinal, String outboundMsgId) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO crw_emission (id, arrival_id, run_date, file_name, state, group_id,"
                        + " batch_ordinal, outbound_msg_id, visible_at) VALUES (?,?,?,?,?,?,?,?,now())",
                id, arrival, RUN, outboundMsgId + "_PAIN008.xml", "VISIBLE", groupId, ordinal, outboundMsgId);
        return id;
    }

    void member(UUID emissionId, int seq, String e2e) {
        jdbc.update("INSERT INTO crw_emission_member (emission_id, sequence, e2e, amount) VALUES (?,?,?,?)",
                emissionId, seq, e2e, 10.00);
    }

    /** agedSeconds null = fresh response; else updated_at is backdated past the debounce. */
    void resp(String table, UUID emissionId, String e2e, String status, Integer agedSeconds) {
        jdbc.update("INSERT INTO " + table + " (response_file, orgnl_msg_id, e2e, status, emission_id)"
                + " VALUES (?,?,?,?,?)", "RESP_" + table + "_" + e2e + ".xml", "MSG", e2e, status, emissionId);
        if (agedSeconds != null) {
            jdbc.update("UPDATE " + table + " SET updated_at = now() - INTERVAL '" + agedSeconds
                    + " seconds' WHERE e2e = ?", e2e);
        }
    }

    ExtRow ext(UUID arrival, String e2e) {
        return jdbc.queryForObject("SELECT status, terminal, emission_id, outbound_msg_id, source_msg_id"
                        + " FROM ext_tx_status WHERE arrival_id = ? AND e2e = ?",
                (rs, i) -> new ExtRow(rs.getString("status"), rs.getBoolean("terminal"),
                        (UUID) rs.getObject("emission_id"), rs.getString("outbound_msg_id"),
                        rs.getString("source_msg_id")), arrival, e2e);
    }

    Map<String, String> due(String client) {
        Map<String, String> rows = new HashMap<>();
        jdbc.query("SELECT source_msg_id, reason FROM prg_report_due WHERE client = ?",
                rs -> { rows.put(rs.getString("source_msg_id"), rs.getString("reason")); }, client);
        return rows;
    }

    // --- (a) batch-scoped status projection ---

    @Test
    void extTxStatusV2NoLongerCrossLinksIdenticalE2eAcrossArrivals() {
        UUID a1 = parent("FNBRF01", "MSGA1");
        tx(a1, 1, "E2EDUP");
        UUID b1 = batch(group(a1, "FNBRF01", "MSGA1", 1), a1, 1, "MSGA1");
        member(b1, 1, "E2EDUP");
        UUID a2 = parent("FNBRF01", "MSGA2");
        tx(a2, 1, "E2EDUP");
        UUID b2 = batch(group(a2, "FNBRF01", "MSGA2", 1), a2, 1, "MSGA2");
        member(b2, 1, "E2EDUP");
        resp("pbsr_resp", b1, "E2EDUP", "ACSC", null); // arrival-1's batch ONLY

        ExtRow hit = ext(a1, "E2EDUP");
        assertThat(hit.status()).isEqualTo("ACSC");
        assertThat(hit.terminal()).isTrue();
        assertThat(hit.emissionId()).isEqualTo(b1);
        assertThat(hit.outboundMsgId()).isEqualTo("MSGA1");
        assertThat(hit.sourceMsgId()).isEqualTo("MSGA1");

        ExtRow other = ext(a2, "E2EDUP"); // A-40 guard: no cross-link
        assertThat(other.status()).isEqualTo("CTV_PASS");
        assertThat(other.terminal()).isFalse();
    }

    @Test
    void legacyNullEmissionResponseStillProjectsFailOpen() {
        UUID a3 = parent("FNBRF01", "MSGA3");
        tx(a3, 1, "E2ELEG");
        resp("isr_resp", null, "E2ELEG", "ACSP", null); // reader could not resolve the batch

        assertThat(ext(a3, "E2ELEG").status()).isEqualTo("ACSP");
    }

    // --- (b) ledger auto-guard + manual bypass ---

    @Test
    void ledgerAutoRowsInsertOncePerClientE2eStatusButManualBypasses() {
        PrgReportEntity first = reports.save(PrgReportEntity.of(
                "FNBCC09", "IMMEDIATE", "COMPLETE", "imm-1", "MSGB", "FNBCC09_PSR_imm-1.txt"));
        ledger.record(first.getId(), "FNBCC09", "E2EB1", "ACSC", null);
        ledger.record(first.getId(), "FNBCC09", "E2EB1", "ACSC", null); // same tuple: silent no-op
        assertThat(ledger.countForReport(first.getId())).isEqualTo(1);

        PrgReportEntity second = reports.save(PrgReportEntity.of(
                "FNBCC09", "IMMEDIATE", "IDLE", "imm-2", "MSGB", "FNBCC09_PSR_imm-2.txt"));
        ledger.record(second.getId(), "FNBCC09", "E2EB1", "ACSC", null); // cross-report auto dedupe
        assertThat(ledger.countForReport(second.getId())).isZero();

        ledger.record(second.getId(), "FNBCC09", "E2EB1", "ACSC", "OPS-1"); // manual: bypass, still audited
        assertThat(ledger.countForReport(second.getId())).isEqualTo(1);
        assertThat(ledger.rowsForReport(second.getId())).containsExactly(new LedgerRow("E2EB1", "ACSC"));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_delivery_ledger WHERE client='FNBCC09'"
                + " AND e2e='E2EB1' AND status='ACSC' AND manual_ref IS NULL", Long.class)).isEqualTo(1L);

        assertThat(reports.findByClientAndParentSourceMsgId("FNBCC09", "MSGB")).hasSize(2);
        assertThat(reports.findById(first.getId())).isPresent();
    }

    // --- (c) the due-view contract ---

    @Test
    void reportDueListsCompleteImmediatelyAndIdleAfterDebounceOnly() {
        String client = "FNBCC01";

        UUID ac = parent(client, "MSGC"); // COMPLETE: every member terminal, responses FRESH
        tx(ac, 1, "EC1");
        tx(ac, 2, "EC2");
        UUID bc = batch(group(ac, client, "MSGC", 1), ac, 1, "MSGC");
        member(bc, 1, "EC1");
        member(bc, 2, "EC2");
        resp("pbsr_resp", bc, "EC1", "ACSC", null);
        resp("pbsr_resp", bc, "EC2", "RJCT", null);

        UUID ai = parent(client, "MSGI"); // IDLE: interim response aged past 120s, EI2 still pending
        tx(ai, 1, "EI1");
        tx(ai, 2, "EI2");
        UUID bi = batch(group(ai, client, "MSGI", 1), ai, 1, "MSGI");
        member(bi, 1, "EI1");
        member(bi, 2, "EI2");
        resp("isr_resp", bi, "EI1", "ACSP", 300);

        UUID ar = parent(client, "MSGR"); // recent response: debounce still running, NOT due
        tx(ar, 1, "ER1");
        UUID br = batch(group(ar, client, "MSGR", 1), ar, 1, "MSGR");
        member(br, 1, "ER1");
        resp("isr_resp", br, "ER1", "ACSP", null);

        UUID al = parent(client, "MSGL"); // complete but fully ledgered: nothing unreported, NOT due
        tx(al, 1, "EL1");
        UUID bl = batch(group(al, client, "MSGL", 1), al, 1, "MSGL");
        member(bl, 1, "EL1");
        resp("pbsr_resp", bl, "EL1", "ACSC", null);
        PrgReportEntity done = reports.save(PrgReportEntity.of(
                client, "IMMEDIATE", "COMPLETE", "imm-c", "MSGL", client + "_PSR_imm-c.txt"));
        ledger.record(done.getId(), client, "EL1", "ACSC", null);

        UUID az = parent(client, "MSGZ"); // zero responses: debounce never started, NOT due
        tx(az, 1, "EZ1");
        UUID bz = batch(group(az, client, "MSGZ", 1), az, 1, "MSGZ");
        member(bz, 1, "EZ1");

        assertThat(due(client)).containsOnly(entry("MSGC", "COMPLETE"), entry("MSGI", "IDLE"));
    }

    // --- (d) unknown status code fails closed as interim ---

    @Test
    void unclassifiedStatusCodeFailsClosedAsInterim() {
        UUID au = parent("FNBRF02", "MSGU");
        tx(au, 1, "EU1");
        UUID bu = batch(group(au, "FNBRF02", "MSGU", 1), au, 1, "MSGU");
        member(bu, 1, "EU1");
        resp("pbsr_resp", bu, "EU1", "ACWC", 300); // ACWC deliberately absent from prg_status_class

        ExtRow row = ext(au, "EU1");
        assertThat(row.status()).isEqualTo("ACWC");
        assertThat(row.terminal()).isFalse();
        assertThat(due("FNBRF02")).containsOnly(entry("MSGU", "IDLE")); // never COMPLETE on unclassified

        assertThat(jdbc.queryForObject(
                "SELECT terminal FROM prg_status_class WHERE code='ACSC'", Boolean.class)).isTrue();
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM prg_status_class", Long.class)).isEqualTo(12L);
    }

    // --- (e) SLA pending view ages non-terminal members of VISIBLE batches ---

    @Test
    void slaPendingAgesOnlyNonTerminalMembers() {
        String client = "FNBRF03";
        UUID as = parent(client, "MSGS");
        tx(as, 1, "ES1");
        tx(as, 2, "ES2");
        UUID bs = batch(group(as, client, "MSGS", 1), as, 1, "MSGS");
        member(bs, 1, "ES1");
        member(bs, 2, "ES2");
        jdbc.update("UPDATE crw_emission SET visible_at = now() - INTERVAL '21 hours' WHERE id = ?", bs);
        resp("pbsr_resp", bs, "ES1", "ACSC", null); // terminal: never SLA-pending

        var rows = jdbc.query("SELECT e2e, outbound_msg_id, age_hours FROM prg_sla_pending WHERE client = ?",
                (rs, i) -> Map.entry(rs.getString("e2e"), rs.getDouble("age_hours")), client);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getKey()).isEqualTo("ES2");
        assertThat(rows.get(0).getValue()).isBetween(20.9, 22.0);
    }
}
