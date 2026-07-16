package za.co.fnb.dcre.prg.data;

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

import static org.assertj.core.api.Assertions.assertThat;

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
 * prg_databasechangelog history tables).
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
            "prg_member_status", "ext_tx_status", "prg_report_due", "prg_sla_pending");

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

    /** Production integration: SpringLiquibase + the per-service history tables. */
    void migrate(final JdbcTemplate jdbc, final String master) throws LiquibaseException {
        final SpringLiquibase liquibase = new SpringLiquibase();
        liquibase.setDataSource(jdbc.getDataSource());
        liquibase.setChangeLog(master);
        liquibase.setDatabaseChangeLogTable("prg_databasechangelog");
        liquibase.setDatabaseChangeLogLockTable("prg_databasechangeloglock");
        liquibase.setResourceLoader(new DefaultResourceLoader());
        liquibase.afterPropertiesSet();
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
