package za.co.fnb.dcre.crg.data;

import liquibase.exception.LiquibaseException;
import liquibase.integration.spring.SpringLiquibase;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SCRUM-55 legacy-state migration gate (testing.md: migration changes are
 * tested against LEGACY database states, not only fresh containers). A fresh
 * Testcontainers DB proves the clean path and nothing else: the 97bcebf view
 * stack rebuild ships upgrade drop changesets whose behavior DIFFERS by
 * history (MARK_RAN on a fresh DB, EXECUTED on a database migrated before the
 * perf fix). This IT replays that reachable historical state from the frozen
 * fixture (src/test/resources/legacy, verbatim 97bcebf~1) and proves the
 * CURRENT changelog converges on it with no checksum or view-dependency
 * errors, using the production Liquibase integration (SpringLiquibase, the
 * prg_databasechangelog history tables, RETAINED under the CRG name: see the
 * [!CONVENTION-OVERRIDE] in application.yml).
 */
class LegacyUpgradeIT {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    static final String LEGACY_MASTER = "classpath:legacy/db.changelog-legacy-master.xml";
    static final String CURRENT_MASTER = "classpath:db/changelog/db.changelog-master.xml";
    static final List<String> V3_VIEWS = List.of("prg_isr_pick", "prg_sbsr_pick", "prg_pbsr_pick",
            "prg_member_status", "ext_tx_status", "prg_report_due", "prg_sla_pending",
            "prg_status_exception");

    @Test
    void legacyV2EndStateConvergesToV3UnderTheCurrentChangelog() throws LiquibaseException {
        final JdbcTemplate jdbc = database("legacy_state");
        migrate(jdbc, LEGACY_MASTER);  // the pre-97bcebf end state: v2 view stack applied
        migrate(jdbc, CURRENT_MASTER); // throws on checksum or view-dependency errors

        assertThat(views(jdbc)).containsAll(V3_VIEWS);
        // The live-upgrade drops must have actually EXECUTED against the v2
        // state (this is the path no fresh-container test ever exercises; on
        // fresh DBs the same changesets MARK_RAN, see the sibling test).
        assertThat(exectype(jdbc, "003-drop-report-due-v1-upgrade")).isEqualTo("EXECUTED");
        assertThat(exectype(jdbc, "003-drop-sla-pending-v1-upgrade")).isEqualTo("EXECUTED");
        // runOnChange re-execution carried the ext_tx_status body v2 -> v3.
        assertThat(exectype(jdbc, "003-ext-tx-status-v2")).isEqualTo("RERAN");
        assertThat(count(jdbc, "prg_report_due")).isZero(); // the AGT scan answers post-upgrade
        assertThat(count(jdbc, "ext_tx_status")).isZero();
    }

    @Test
    void freshDatabaseMarksTheUpgradeDropsRanAndTheChangelogStaysIdempotent() throws LiquibaseException {
        final JdbcTemplate jdbc = database("fresh_state");
        migrate(jdbc, CURRENT_MASTER);
        migrate(jdbc, CURRENT_MASTER); // re-run convergence: pure no-op, no checksum errors

        assertThat(views(jdbc)).containsAll(V3_VIEWS);
        assertThat(exectype(jdbc, "003-drop-report-due-v1-upgrade")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "003-drop-sla-pending-v1-upgrade")).isEqualTo("MARK_RAN");
        assertThat(count(jdbc, "prg_report_due")).isZero();
    }

    @Test
    void halfAppliedStatusClassificationConvergesUnderTheCurrentChangelog() throws LiquibaseException {
        final JdbcTemplate jdbc = database("half_status_classification");
        migrate(jdbc, LEGACY_MASTER);

        // Reachable interrupted-DDL state: catalogue columns/rows and one
        // constraint stand, but Liquibase never recorded any 004 changeset.
        jdbc.execute("ALTER TABLE prg_status_class ADD COLUMN classification VARCHAR(32)");
        jdbc.execute("ALTER TABLE prg_status_class ADD COLUMN reportable BOOLEAN");
        jdbc.update("UPDATE prg_status_class SET classification='TERMINAL_SUCCESS', reportable=true"
                + " WHERE code IN ('ACSC','ACCC')");
        jdbc.update("UPDATE prg_status_class SET classification='TERMINAL_NON_SUCCESS', reportable=true"
                + " WHERE code IN ('RJCT','CANC')");
        jdbc.update("UPDATE prg_status_class SET classification='ACCEPTED_NON_TERMINAL', reportable=true"
                + " WHERE code IN ('ACSP','ACTC','ACCP','ACFC')");
        jdbc.update("UPDATE prg_status_class SET classification='PENDING_INTERIM', reportable=true"
                + " WHERE code IN ('RCVD','PDNG','PART','PATC')");
        jdbc.update("INSERT INTO prg_status_class(code,terminal,classification,reportable)"
                + " VALUES ('ACWC',false,'UNSUPPORTED',false),('ACWP',false,'UNSUPPORTED',false)");
        jdbc.execute("ALTER TABLE prg_status_class ALTER COLUMN classification SET NOT NULL");
        jdbc.execute("""
                CREATE VIEW prg_status_exception AS
                SELECT x.client::VARCHAR(16) AS client,
                       COALESCE(x.source_msg_id, h.msg_id)::VARCHAR(35) AS source_msg_id,
                       x.outbound_msg_id::VARCHAR(64) AS outbound_msg_id,
                       x.e2e::VARCHAR(35) AS e2e, x.status::VARCHAR(32) AS status,
                       COALESCE(sc.classification, 'UNKNOWN')::VARCHAR(32) AS classification
                FROM ext_tx_status x
                JOIN tx_header h ON h.arrival_id = x.arrival_id
                LEFT JOIN prg_status_class sc ON sc.code = x.status
                WHERE x.status IS NOT NULL AND x.stage_rank > 1
                  AND NOT COALESCE(sc.reportable, false)
                """);

        migrate(jdbc, CURRENT_MASTER);
        migrate(jdbc, CURRENT_MASTER);

        assertThat(exectype(jdbc, "004-status-classification-column")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "004-status-reportable-column")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "004-classify-acwc-unsupported")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "004-classify-acwp-unsupported")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "004-status-classification-not-null")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "004-status-reportable-not-null")).isEqualTo("EXECUTED");
        // SCRUM-68: the 005 warehoused-interim changesets run on the half-applied path.
        assertThat(exectype(jdbc, "005-sla-suppressed-column")).isEqualTo("EXECUTED");
        assertThat(exectype(jdbc, "005-sla-suppressed-backfill")).isEqualTo("EXECUTED");
        assertThat(exectype(jdbc, "005-sla-suppressed-not-null")).isEqualTo("EXECUTED");
        assertThat(exectype(jdbc, "005-reclassify-acwp-acwc")).isEqualTo("EXECUTED");
        assertThat(exectype(jdbc, "005-sla-pending-suppression")).isEqualTo("EXECUTED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_status_class", Long.class)).isEqualTo(14L);
        assertThat(jdbc.queryForMap("SELECT classification, terminal, reportable, sla_suppressed"
                + " FROM prg_status_class WHERE code='ACWC'"))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "classification", "ACCEPTED_NON_TERMINAL", "terminal", false,
                        "reportable", true, "sla_suppressed", true));
        assertThat(jdbc.queryForObject("SELECT view_definition FROM information_schema.views"
                + " WHERE table_schema='public' AND table_name='prg_status_exception'", String.class))
                .contains("ext_tx_status");
        assertThat(jdbc.queryForObject("SELECT view_definition FROM information_schema.views"
                + " WHERE table_schema='public' AND table_name='prg_sla_pending'", String.class))
                .contains("sla_suppressed");
    }

    @Test
    void status004EndStateReclassifiesWarehousedAndReownsSlaPending() throws LiquibaseException {
        final JdbcTemplate jdbc = database("end_state_004");
        migrate(jdbc, LEGACY_MASTER);

        // The exact 004 end state: both catalogue columns present and NOT NULL,
        // all fourteen rows classified with ACWC/ACWP standing as the 004
        // UNSUPPORTED/non-reportable rows, no 005 changeset recorded.
        jdbc.execute("ALTER TABLE prg_status_class ADD COLUMN classification VARCHAR(32)");
        jdbc.execute("ALTER TABLE prg_status_class ADD COLUMN reportable BOOLEAN");
        jdbc.update("UPDATE prg_status_class SET classification='TERMINAL_SUCCESS', reportable=true"
                + " WHERE code IN ('ACSC','ACCC')");
        jdbc.update("UPDATE prg_status_class SET classification='TERMINAL_NON_SUCCESS', reportable=true"
                + " WHERE code IN ('RJCT','CANC')");
        jdbc.update("UPDATE prg_status_class SET classification='ACCEPTED_NON_TERMINAL', reportable=true"
                + " WHERE code IN ('ACSP','ACTC','ACCP','ACFC')");
        jdbc.update("UPDATE prg_status_class SET classification='PENDING_INTERIM', reportable=true"
                + " WHERE code IN ('RCVD','PDNG','PART','PATC')");
        jdbc.update("INSERT INTO prg_status_class(code,terminal,classification,reportable)"
                + " VALUES ('ACWC',false,'UNSUPPORTED',false),('ACWP',false,'UNSUPPORTED',false)");
        jdbc.execute("ALTER TABLE prg_status_class ALTER COLUMN classification SET NOT NULL");
        jdbc.execute("ALTER TABLE prg_status_class ALTER COLUMN reportable SET NOT NULL");

        migrate(jdbc, CURRENT_MASTER);
        migrate(jdbc, CURRENT_MASTER); // idempotent second run: no checksum errors, no re-runs

        // Premise pins: the seeded state IS 004's end state, so every guarded
        // 004 changeset marks ran instead of re-executing.
        assertThat(exectype(jdbc, "004-classify-acwc-unsupported")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "004-status-classification-not-null")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "004-status-reportable-not-null")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "005-sla-suppressed-column")).isEqualTo("EXECUTED");
        assertThat(exectype(jdbc, "005-sla-suppressed-backfill")).isEqualTo("EXECUTED");
        assertThat(exectype(jdbc, "005-sla-suppressed-not-null")).isEqualTo("EXECUTED");
        assertThat(exectype(jdbc, "005-reclassify-acwp-acwc")).isEqualTo("EXECUTED");
        assertThat(exectype(jdbc, "005-sla-pending-suppression")).isEqualTo("EXECUTED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_status_class", Long.class)).isEqualTo(14L);
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM prg_status_class WHERE sla_suppressed", Long.class)).isEqualTo(2L);
        assertThat(jdbc.queryForMap("SELECT classification, terminal, reportable, sla_suppressed"
                + " FROM prg_status_class WHERE code='ACWP'"))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "classification", "ACCEPTED_NON_TERMINAL", "terminal", false,
                        "reportable", true, "sla_suppressed", true));
        assertThat(jdbc.queryForObject("SELECT view_definition FROM information_schema.views"
                + " WHERE table_schema='public' AND table_name='prg_sla_pending'", String.class))
                .contains("sla_suppressed");
    }

    @Test
    void interrupted005DdlStateConvergesUnderTheCurrentChangelog() throws LiquibaseException {
        final JdbcTemplate jdbc = database("half_warehoused_005");
        migrate(jdbc, LEGACY_MASTER);

        // 004 end state first (as in the sibling arm) ...
        jdbc.execute("ALTER TABLE prg_status_class ADD COLUMN classification VARCHAR(32)");
        jdbc.execute("ALTER TABLE prg_status_class ADD COLUMN reportable BOOLEAN");
        jdbc.update("UPDATE prg_status_class SET classification='TERMINAL_SUCCESS', reportable=true"
                + " WHERE code IN ('ACSC','ACCC')");
        jdbc.update("UPDATE prg_status_class SET classification='TERMINAL_NON_SUCCESS', reportable=true"
                + " WHERE code IN ('RJCT','CANC')");
        jdbc.update("UPDATE prg_status_class SET classification='ACCEPTED_NON_TERMINAL', reportable=true"
                + " WHERE code IN ('ACSP','ACTC','ACCP','ACFC')");
        jdbc.update("UPDATE prg_status_class SET classification='PENDING_INTERIM', reportable=true"
                + " WHERE code IN ('RCVD','PDNG','PART','PATC')");
        jdbc.update("INSERT INTO prg_status_class(code,terminal,classification,reportable)"
                + " VALUES ('ACWC',false,'UNSUPPORTED',false),('ACWP',false,'UNSUPPORTED',false)");
        jdbc.execute("ALTER TABLE prg_status_class ALTER COLUMN classification SET NOT NULL");
        jdbc.execute("ALTER TABLE prg_status_class ALTER COLUMN reportable SET NOT NULL");

        // ... then the reachable mid-005 kill state: column added, backfilled,
        // NOT NULL standing and the rows already reclassified, but Liquibase
        // never recorded ANY 005 changeset (CRDB commits DDL per statement).
        jdbc.execute("ALTER TABLE prg_status_class ADD COLUMN sla_suppressed BOOLEAN");
        jdbc.update("UPDATE prg_status_class SET sla_suppressed = false WHERE sla_suppressed IS NULL");
        jdbc.execute("ALTER TABLE prg_status_class ALTER COLUMN sla_suppressed SET NOT NULL");
        jdbc.update("UPDATE prg_status_class SET classification='ACCEPTED_NON_TERMINAL',"
                + " reportable=true, sla_suppressed=true WHERE code IN ('ACWC','ACWP')");

        migrate(jdbc, CURRENT_MASTER);
        migrate(jdbc, CURRENT_MASTER);

        assertThat(exectype(jdbc, "005-sla-suppressed-column")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "005-sla-suppressed-not-null")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "005-sla-suppressed-backfill")).isEqualTo("EXECUTED");
        assertThat(exectype(jdbc, "005-reclassify-acwp-acwc")).isEqualTo("EXECUTED");
        assertThat(exectype(jdbc, "005-sla-pending-suppression")).isEqualTo("EXECUTED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_status_class", Long.class)).isEqualTo(14L);
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM prg_status_class WHERE sla_suppressed", Long.class)).isEqualTo(2L);
        assertThat(jdbc.queryForMap("SELECT classification, terminal, reportable, sla_suppressed"
                + " FROM prg_status_class WHERE code='ACWC'"))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "classification", "ACCEPTED_NON_TERMINAL", "terminal", false,
                        "reportable", true, "sla_suppressed", true));
        assertThat(jdbc.queryForObject("SELECT view_definition FROM information_schema.views"
                + " WHERE table_schema='public' AND table_name='prg_sla_pending'", String.class))
                .contains("sla_suppressed");
    }

    /**
     * SCRUM-107 cutover: the state the PRG -> CRG rename creates, which no other arm reaches. A
     * live dcre_col already carries the full v3 schema and its history. CRG ships renamed
     * changeset ids ({@code 003-crg-report}) in renamed files ({@code 001-crg.xml}), and
     * changeset identity is (id, author, filename), so every renamed changeset is NEW even
     * against the retained {@code prg_databasechangelog} and re-evaluates against a built schema.
     *
     * <p>The load-bearing assertion is the watermark. {@code prg_watermark} is live
     * delta-reporting state and is deliberately NOT renamed: if the cutover dropped, recreated or
     * emptied it, the next window would treat the whole book as unreported and re-emit history.
     * A seeded row must survive untouched.
     */
    @Test
    void prgEraDatabaseConvergesUnderTheRenamedChangesetIdentities() throws LiquibaseException {
        final JdbcTemplate jdbc = database("prg_era_cutover");
        migrateInto(jdbc, CURRENT_MASTER, "prg_databasechangelog");

        // That built the v3 schema, but it recorded the RENAMED identities, so as it stands this
        // is a crg-era database and the cutover it is supposed to model has already happened.
        // Rewrite the history back to the pre-rename identities. The rename was purely the token
        // prg -> crg in ids and changelog filenames, and no pre-rename identity contained "crg",
        // so reversing it is exact rather than approximate.
        final int rewritten = jdbc.update("UPDATE prg_databasechangelog"
                + " SET id = replace(id, 'crg', 'prg'), filename = replace(filename, 'crg', 'prg')"
                + " WHERE id LIKE '%crg%' OR filename LIKE '%crg%'");
        assertThat(rewritten)
                .as("the fixture rewrote no identities, so it is not a prg-era database and this"
                        + " test would pass without exercising the cutover at all")
                .isPositive();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM prg_databasechangelog"
                + " WHERE id LIKE '%crg%' OR filename LIKE '%crg%'", Long.class))
                .as("no renamed identity may survive the rewrite")
                .isZero();

        jdbc.update("INSERT INTO prg_watermark(client, e2e, last_status)"
                + " VALUES ('FNBCC01', 'E2E-CUTOVER-0001', 'ACSP')");
        jdbc.update("INSERT INTO prg_report(id, client, report_type, trigger_kind, window_key,"
                + " file_name, created_at) VALUES (gen_random_uuid(), 'FNBCC01', 'SCHEDULED',"
                + " 'CLOCK', 'w1', 'FNBCC01_PSR_w1.txt', now())");
        // The pre-rename service ran its Batch metadata under PRG_BATCH_, and dropped CRG_BATCH_,
        // which the renamed changelog above created. Swap them so the prefixes match the era too.
        jdbc.execute("DROP TABLE crg_batch_job_instance CASCADE");
        jdbc.execute("CREATE TABLE PRG_BATCH_JOB_INSTANCE (JOB_INSTANCE_ID BIGINT NOT NULL"
                + " PRIMARY KEY, VERSION BIGINT, JOB_NAME VARCHAR(100) NOT NULL,"
                + " JOB_KEY VARCHAR(32) NOT NULL)");
        jdbc.update("INSERT INTO PRG_BATCH_JOB_INSTANCE(JOB_INSTANCE_ID, VERSION, JOB_NAME,"
                + " JOB_KEY) VALUES (1, 0, 'prgJob', 'cutover')");
        final long historyBefore = historyCount(jdbc, "prg_databasechangelog");

        migrateInto(jdbc, CURRENT_MASTER, "prg_databasechangelog"); // CRG's first start
        migrateInto(jdbc, CURRENT_MASTER, "prg_databasechangelog"); // and a restart: still a no-op

        // The watermark survived. This is the production-incident assertion, not a schema one.
        assertThat(jdbc.queryForMap(
                "SELECT client, e2e, last_status FROM prg_watermark WHERE e2e = 'E2E-CUTOVER-0001'"))
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "client", "FNBCC01", "e2e", "E2E-CUTOVER-0001", "last_status", "ACSP"));
        assertThat(count(jdbc, "prg_watermark")).isEqualTo(1L);
        assertThat(count(jdbc, "prg_report")).isEqualTo(1L);

        // Schema converged and the renamed batch metadata was created alongside the old prefix.
        assertThat(views(jdbc)).containsAll(V3_VIEWS);
        assertThat(batchTables(jdbc, "crg_batch_job_instance")).isEqualTo(1L);
        assertThat(batchTables(jdbc, "prg_batch_job_instance"))
                .as("the PRG_BATCH_ metadata is orphaned by the prefix rename, never dropped")
                .isEqualTo(1L);
        assertThat(count(jdbc, "prg_batch_job_instance"))
                .as("orphaned means left intact, so the old rows are still readable")
                .isEqualTo(1L);

        // The renamed identities were recorded as ADDITIONAL rows: the pre-rename rows are still
        // there, so history was appended to, never rewritten. Exactly `rewritten` of them are new.
        assertThat(historyCount(jdbc, "prg_databasechangelog"))
                .isEqualTo(historyBefore + rewritten);
        // And each renamed changeset was a guarded no-op against the already-built schema rather
        // than a re-execution, which is the property that makes the identity rename safe at all.
        assertThat(exectype(jdbc, "003-crg-report")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "003-crg-delivery-ledger")).isEqualTo("MARK_RAN");
        assertThat(exectype(jdbc, "004-ix-crg-report-job")).isEqualTo("MARK_RAN");
    }

    /**
     * Red-proof for the {@code [!CONVENTION-OVERRIDE]} in application.yml. SCRUM-107 was asked to
     * rename the Liquibase history table to {@code crg_databasechangelog} alongside everything
     * else. It cannot be done: a history table IS the migration state, so a renamed one is EMPTY
     * against a fully-built dcre_col and replays all 53 changesets, including the v1 view drops
     * that only ever ran before the dependent views existed. CRG would crash-loop on startup.
     *
     * <p>This test exists so the reason is executable rather than a comment somebody deletes. If a
     * future change makes the replay genuinely safe, this test goes red and the override can be
     * revisited on evidence.
     */
    @Test
    void renamingTheHistoryTableWouldReplayTheChangelogAndFail() throws LiquibaseException {
        final JdbcTemplate jdbc = database("history_rename_refused");
        migrateInto(jdbc, CURRENT_MASTER, "prg_databasechangelog");

        assertThatThrownBy(() -> migrateInto(jdbc, CURRENT_MASTER, "crg_databasechangelog"))
                .rootCause()
                .hasMessageContaining("cannot drop relation \"ext_tx_status\"")
                .hasMessageContaining("prg_status_exception");
    }

    /** One logical database per scenario inside the shared container. */
    JdbcTemplate database(final String name) {
        new JdbcTemplate(dataSource(CRDB.getDatabaseName()))
                .execute("CREATE DATABASE IF NOT EXISTS " + name);
        return new JdbcTemplate(dataSource(name));
    }

    DataSource dataSource(final String name) {
        final String url = CRDB.getJdbcUrl().replace("/" + CRDB.getDatabaseName(), "/" + name);
        return new DriverManagerDataSource(url, CRDB.getUsername(), CRDB.getPassword());
    }

    /** Production integration: SpringLiquibase + this service's own history tables. */
    void migrate(final JdbcTemplate jdbc, final String master) throws LiquibaseException {
        migrateInto(jdbc, master, "prg_databasechangelog");
    }

    /**
     * SpringLiquibase against a NAMED history table, so a scenario can replay the pre-SCRUM-107
     * service (history in {@code prg_databasechangelog}) and then the renamed one against the same
     * schema. The lock table is derived the way the service config derives it.
     */
    void migrateInto(final JdbcTemplate jdbc, final String master, final String historyTable)
            throws LiquibaseException {
        final SpringLiquibase liquibase = new SpringLiquibase();
        liquibase.setDataSource(jdbc.getDataSource());
        liquibase.setChangeLog(master);
        liquibase.setDatabaseChangeLogTable(historyTable);
        liquibase.setDatabaseChangeLogLockTable(historyTable + "lock");
        liquibase.setResourceLoader(new DefaultResourceLoader());
        liquibase.afterPropertiesSet();
    }

    long batchTables(final JdbcTemplate jdbc, final String table) {
        final Long rows = jdbc.queryForObject("SELECT count(*) FROM information_schema.tables"
                + " WHERE table_schema = 'public' AND table_name = ?", Long.class, table);
        return rows == null ? -1 : rows;
    }

    long historyCount(final JdbcTemplate jdbc, final String historyTable) {
        final Long rows = jdbc.queryForObject("SELECT count(*) FROM " + historyTable, Long.class);
        return rows == null ? -1 : rows;
    }

    List<String> views(final JdbcTemplate jdbc) {
        return jdbc.queryForList(
                "SELECT table_name FROM information_schema.views WHERE table_schema = 'public'",
                String.class);
    }

    String exectype(final JdbcTemplate jdbc, final String changesetId) {
        return jdbc.queryForObject(
                "SELECT exectype FROM prg_databasechangelog WHERE id = ?", String.class, changesetId);
    }

    long count(final JdbcTemplate jdbc, final String view) {
        final Long rows = jdbc.queryForObject("SELECT count(*) FROM " + view, Long.class);
        return rows == null ? -1 : rows;
    }
}
