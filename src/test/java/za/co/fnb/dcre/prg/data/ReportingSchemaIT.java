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
 *     arrival no longer cross-links a response (A-40 guard); a response with
 *     emission_id NULL projects ONLY when its arrival has NO emission at all
 *     (legacy/pre-split truth, review prg-12) and only within its parent
 *     identity family (orgnl_msg_id = parent MsgId or a split child MsgId_N);
 *     per response table only the LATEST row per (emission, e2e) projects
 *     (max created_at, response_file tiebreaker), so a resend or a second
 *     response file for one emission never multiplies rows or flip-flops the
 *     status pick; prg_sla_pending reads ext_tx_status and inherits this.
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
        resp(table, emissionId, "MSG", e2e, status, agedSeconds);
    }

    /**
     * orgnl-aware variant: NULL-emission rows only project inside the parent
     * identity family, so unresolved seeds must carry a realistic OrgnlMsgId.
     * response_file discriminates resolved vs legacy twins of one e2e
     * (UNIQUE (response_file, e2e)).
     */
    void resp(String table, UUID emissionId, String orgnlMsgId, String e2e, String status,
              Integer agedSeconds) {
        jdbc.update("INSERT INTO " + table + " (response_file, orgnl_msg_id, e2e, status, emission_id)"
                        + " VALUES (?,?,?,?,?)",
                "RESP_%s_%s_%s.xml".formatted(table, e2e, emissionId == null ? "legacy" : "batch"),
                orgnlMsgId, e2e, status, emissionId);
        if (agedSeconds != null) {
            jdbc.update("UPDATE " + table + " SET updated_at = now() - INTERVAL '" + agedSeconds
                    + " seconds' WHERE e2e = ?", e2e);
        }
    }

    /**
     * Latest-row fixture seed: explicit response_file plus a backdated
     * created_at (and updated_at, for the due-view debounce) so resend twins
     * of one (emission, e2e) carry a deterministic recency order.
     */
    void respAt(String table, UUID emissionId, String orgnlMsgId, String e2e, String status,
                String responseFile, int createdAgoSeconds) {
        jdbc.update("INSERT INTO " + table + " (response_file, orgnl_msg_id, e2e, status, emission_id)"
                        + " VALUES (?,?,?,?,?)", responseFile, orgnlMsgId, e2e, status, emissionId);
        jdbc.update("UPDATE " + table + " SET created_at = now() - INTERVAL '" + createdAgoSeconds
                + " seconds', updated_at = now() - INTERVAL '" + createdAgoSeconds
                + " seconds' WHERE response_file = ?", responseFile);
    }

    ExtRow ext(UUID arrival, String e2e) {
        return jdbc.queryForObject("SELECT status, terminal, emission_id, outbound_msg_id, source_msg_id"
                        + " FROM ext_tx_status WHERE arrival_id = ? AND e2e = ?",
                (rs, i) -> new ExtRow(rs.getString("status"), rs.getBoolean("terminal"),
                        (UUID) rs.getObject("emission_id"), rs.getString("outbound_msg_id"),
                        rs.getString("source_msg_id")), arrival, e2e);
    }

    long extCount(UUID arrival, String e2e) {
        Long count = jdbc.queryForObject("SELECT count(*) FROM ext_tx_status WHERE arrival_id = ?"
                + " AND e2e = ?", Long.class, arrival, e2e);
        return count == null ? -1 : count;
    }

    long slaCount(String client) {
        Long count = jdbc.queryForObject("SELECT count(*) FROM prg_sla_pending WHERE client = ?",
                Long.class, client);
        return count == null ? -1 : count;
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
        resp("isr_resp", null, "MSGA3", "E2ELEG", "ACSP", null); // reader could not resolve the batch

        assertThat(ext(a3, "E2ELEG").status()).isEqualTo("ACSP");

        UUID a4 = parent("FNBRF01", "MSGA4"); // split child reply, registry still behind
        tx(a4, 1, "E2ELEG2");
        resp("isr_resp", null, "MSGA4_2", "E2ELEG2", "ACSP", null);

        assertThat(ext(a4, "E2ELEG2").status()).isEqualTo("ACSP");
    }

    @Test
    void nullEmissionFallbackOnlyMatchesWhenNoEmissionExistsForTheArrival() {
        // A-40 guard shape (review prg-12): the SAME e2e in TWO arrivals;
        // arrival-1 resolved a batch, arrival-2 is legacy (no emission at all).
        UUID a1 = parent("FNBRF04", "MSGN1");
        tx(a1, 1, "E2ENUL");
        UUID b1 = batch(group(a1, "FNBRF04", "MSGN1", 1), a1, 1, "MSGN1");
        member(b1, 1, "E2ENUL");
        UUID a2 = parent("FNBRF04", "MSGN2");
        tx(a2, 1, "E2ENUL"); // deliberately NO emission: pre-split legacy data
        respAt("pbsr_resp", null, "MSGN1", "E2ENUL", "ACSC", "RESP_NUL_P1.xml", 0); // unresolved, names parent-1

        // parent-1 resolved a batch: batch-scoped responses are the only truth
        // there; the unresolved row must neither stand in alongside the batch
        // nor cross-link to parent-2 (whose family it does not name).
        assertThat(ext(a1, "E2ENUL").status()).isEqualTo("CTV_PASS");
        assertThat(ext(a1, "E2ENUL").terminal()).isFalse();
        assertThat(ext(a2, "E2ENUL").status()).isEqualTo("CTV_PASS");

        respAt("pbsr_resp", null, "MSGN2", "E2ENUL", "ACSP", "RESP_NUL_P2.xml", 0); // legacy, names parent-2

        assertThat(ext(a2, "E2ENUL").status()).isEqualTo("ACSP"); // no emission at all: fail-open truth
        assertThat(extCount(a1, "E2ENUL")).isEqualTo(1L);
        assertThat(extCount(a2, "E2ENUL")).isEqualTo(1L);
    }

    @Test
    void secondResponseFileForTheSameEmissionProjectsOnlyTheNewestStatus() {
        String client = "FNBRF06";
        UUID a = parent(client, "MSGRS1");
        tx(a, 1, "E2ERSND");
        UUID b = batch(group(a, client, "MSGRS1", 1), a, 1, "MSGRS1");
        member(b, 1, "E2ERSND");
        respAt("pbsr_resp", b, "MSGRS1", "E2ERSND", "ACSP", "RESP_RSND_FIRST.xml", 300);

        assertThat(ext(a, "E2ERSND").status()).isEqualTo("ACSP");
        assertThat(slaCount(client)).isEqualTo(1L); // interim member of a VISIBLE batch ages

        // resend: a SECOND response file arrives for the SAME (emission, e2e)
        respAt("pbsr_resp", b, "MSGRS1", "E2ERSND", "ACSC", "RESP_RSND_SECOND.xml", 0);

        assertThat(extCount(a, "E2ERSND")).isEqualTo(1L); // no row multiplication
        assertThat(ext(a, "E2ERSND").status()).isEqualTo("ACSC"); // newest created_at wins
        assertThat(ext(a, "E2ERSND").terminal()).isTrue();
        assertThat(slaCount(client)).isZero(); // the stale interim row cannot resurrect the member
    }

    @Test
    void equalTimestampResponseTwinsTieBreakDeterministicallyOnResponseFile() {
        String client = "FNBRF07";
        UUID a = parent(client, "MSGTB1");
        tx(a, 1, "E2ETIE");
        UUID b = batch(group(a, client, "MSGTB1", 1), a, 1, "MSGTB1");
        member(b, 1, "E2ETIE");
        respAt("isr_resp", b, "MSGTB1", "E2ETIE", "ACSP", "RESP_TIE_A.xml", 0);
        respAt("isr_resp", b, "MSGTB1", "E2ETIE", "ACTC", "RESP_TIE_B.xml", 0);
        jdbc.update("UPDATE isr_resp SET created_at = '2026-07-16 08:00:00+00' WHERE e2e = 'E2ETIE'");

        assertThat(extCount(a, "E2ETIE")).isEqualTo(1L);
        assertThat(ext(a, "E2ETIE").status()).isEqualTo("ACTC"); // greater response_file wins the tie
    }

    @Test
    void resolvedResponsePrevailsOverItsNullEmissionTwinWithoutRowDuplication() {
        UUID a = parent("FNBRF05", "MSGT1");
        tx(a, 1, "E2ETWIN");
        UUID b = batch(group(a, "FNBRF05", "MSGT1", 1), a, 1, "MSGT1");
        member(b, 1, "E2ETWIN");
        resp("isr_resp", b, "MSGT1", "E2ETWIN", "ACSC", null);    // resolved, batch-scoped
        resp("isr_resp", null, "MSGT1", "E2ETWIN", "ACSP", null); // legacy twin, same identity family

        assertThat(jdbc.queryForObject("SELECT count(*) FROM ext_tx_status WHERE arrival_id = ?"
                + " AND e2e = 'E2ETWIN'", Long.class, a)).isEqualTo(1L); // no double TX/ledger tuples
        assertThat(ext(a, "E2ETWIN").status()).isEqualTo("ACSC");
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
