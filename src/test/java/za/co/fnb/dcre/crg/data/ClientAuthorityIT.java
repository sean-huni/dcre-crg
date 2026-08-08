package za.co.fnb.dcre.crg.data;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;
import za.co.fnb.dcre.crg.data.model.StatusRow;
import za.co.fnb.dcre.crg.data.repo.CrgWatermarkRepo;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A-43, ruled 2026-08-08: CRG's projection stack reads {@code client} from TWO relations, and this
 * class is the only fixture in the repo where they can disagree.
 *
 * <ul>
 *   <li>{@code ext_tx_status.client} is {@code tx_header.client_token}, and the whole delta and
 *       watermark path keys on it ({@code CrgWatermarkRepo.findUnreportedForParent});</li>
 *   <li>{@code prg_report_due.client} and {@code prg_member_status.client} come from
 *       {@code crw_emission_group.client}, which CRW writes.</li>
 * </ul>
 *
 * <p>Every other fixture here seeds one value into {@code initg_pty} and {@code client_token} alike,
 * so the two paths agree by construction and no existing test can tell which column CRW used. That
 * monoculture is why the disagreement survived four weeks and was reported as a defect rather than
 * caught as a failure.
 *
 * <p>The failure it produces is silent by design: {@code prg_report_due} names the parent under one
 * identity, the delta read finds no rows under the other, the immediate report emits nothing,
 * nothing is ledgered, the parent stays due, and AGT retriggers it forever with no error anywhere.
 * {@link #anEmissionGroupWrittenFromTheHeaderProducesTheSilentRetriggerLoop} pins that mechanism so
 * a future reader can see WHY the authority matters rather than being asked to take it on trust.
 */
@SpringBootTest(properties = {
        "spring.liquibase.change-log=classpath:db/changelog/db.changelog-test-master.xml",
        "spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class ClientAuthorityIT {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(final DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
    }

    private static final LocalDate RUN = LocalDate.of(2026, 8, 20);

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    CrgWatermarkRepo watermarks;

    /** A parent whose two client homes are stated separately, so a test can make them differ. */
    private UUID parent(final String clientToken, final String initgPty, final String msgId) {
        final UUID arrival = UUID.randomUUID();
        jdbc.update("INSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,"
                        + " initg_pty, business_date, client_token, layout_version) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, msgId, msgId, "20260820080000", 1, initgPty, "20260820", clientToken, 2);
        return arrival;
    }

    /**
     * One emitted, responded collection line under {@code groupClient}, which is what CRW wrote
     * into {@code crw_emission_group.client}. Stating it as a parameter rather than deriving it is
     * the point of the fixture: it is the value under test.
     */
    private void emittedLine(final UUID arrival, final String groupClient, final String msgId,
                             final String e2e, final String status) {
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,"
                        + " creditor_account, currency, amount_raw, amount) VALUES (?,1,'DC',?,?,?,?,?,?)",
                arrival, e2e, e2e, "62000000010", "ZAR", "1000", 10.00);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,1,'PASS')", arrival);
        final UUID groupId = UUID.randomUUID();
        jdbc.update("INSERT INTO crw_emission_group (id, arrival_id, client, source_msg_id, run_date,"
                        + " applied_max, total_tx, total_amount, expected_batch_count, split)"
                        + " VALUES (?,?,?,?,?,?,?,?,?,false)",
                groupId, arrival, groupClient, msgId, RUN, 5000, 1L, 10.00, 1);
        final UUID emissionId = UUID.randomUUID();
        jdbc.update("INSERT INTO crw_emission (id, arrival_id, run_date, file_name, state, group_id,"
                        + " batch_ordinal, outbound_msg_id, visible_at) VALUES (?,?,?,?,'VISIBLE',?,1,?,now())",
                emissionId, arrival, RUN, msgId + "_PAIN008.xml", groupId, msgId);
        jdbc.update("INSERT INTO crw_emission_member (emission_id, sequence, e2e, amount) VALUES (?,1,?,?)",
                emissionId, e2e, 10.00);
        jdbc.update("INSERT INTO pbsr_resp (response_file, orgnl_msg_id, e2e, status, emission_id)"
                        + " VALUES (?,?,?,?,?)", "RESP_" + e2e + ".xml", msgId, e2e, status, emissionId);
    }

    private String extClient(final UUID arrival, final String e2e) {
        return jdbc.queryForObject("SELECT client FROM ext_tx_status WHERE arrival_id = ? AND e2e = ?",
                String.class, arrival, e2e);
    }

    private List<String> dueClientsFor(final String sourceMsgId) {
        return jdbc.queryForList("SELECT client FROM prg_report_due WHERE source_msg_id = ?",
                String.class, sourceMsgId);
    }

    private List<String> memberStatusClientsFor(final String sourceMsgId) {
        return jdbc.queryForList("SELECT client FROM prg_member_status WHERE source_msg_id = ?",
                String.class, sourceMsgId);
    }

    /**
     * The ruling's contract, end to end: with CRW writing the filename token, the emission-group
     * side and the header side of the projection name the SAME client, and the parent's delta is
     * readable under the client the due view reported.
     */
    @Test
    void withTheRuledAuthorityEveryProjectionNamesOneClient() {
        final String msgId = "DCRECC2026082000000301";
        final UUID arrival = parent("FNBCC02", "FNBRF01", msgId);
        emittedLine(arrival, "FNBCC02", msgId, "E2ECA1", "RJCT");

        assertThat(extClient(arrival, "E2ECA1"))
                .as("ext_tx_status.client is tx_header.client_token")
                .isEqualTo("FNBCC02");
        assertThat(memberStatusClientsFor(msgId)).containsExactly("FNBCC02");
        assertThat(dueClientsFor(msgId))
                .as("prg_report_due names the parent under the same client the delta read uses")
                .containsExactly("FNBCC02");

        final List<StatusRow> delta = watermarks.findUnreportedForParent("FNBCC02", msgId, 100);
        assertThat(delta).extracting(StatusRow::e2e).containsExactly("E2ECA1");
    }

    /**
     * The defect, characterised. This seeds the PRE-RULING write (emission group carrying
     * {@code initg_pty}) and asserts the two halves disagree, which is what makes the report emit
     * nothing while the parent stays permanently due.
     *
     * <p>It asserts the DISAGREEMENT rather than a failure, because there is no failure to assert:
     * that is the entire problem. Nothing throws, nothing logs, and both queries are individually
     * correct. Delete this test only when the emission group can no longer carry a client the
     * header side does not know.
     */
    @Test
    void anEmissionGroupWrittenFromTheHeaderProducesTheSilentRetriggerLoop() {
        final String msgId = "DCRECC2026082000000302";
        final UUID arrival = parent("FNBCC02", "FNBRF01", msgId);
        emittedLine(arrival, "FNBRF01", msgId, "E2ECA2", "RJCT");

        assertThat(dueClientsFor(msgId))
                .as("the due view reports the parent under the emission group's client")
                .containsExactly("FNBRF01");
        assertThat(extClient(arrival, "E2ECA2"))
                .as("the status projection carries the header's client_token")
                .isEqualTo("FNBCC02");

        assertThat(watermarks.findUnreportedForParent("FNBRF01", msgId, 100))
                .as("the delta read under the reported client finds nothing: the report emits"
                        + " nothing, nothing is ledgered, and the parent stays due forever")
                .isEmpty();
        assertThat(watermarks.findUnreportedForParent("FNBCC02", msgId, 100))
                .as("the rows exist, under an identity the due view never reports")
                .extracting(StatusRow::e2e)
                .containsExactly("E2ECA2");
    }
}
