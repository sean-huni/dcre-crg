package za.co.fnb.dcre.crg.data;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A-70 (SCRUM-78, M10): the dcre_col {@code man_collection_outcome} view is the
 * REAL source for MSR's suspension sweep (consecutive terminal-failed collections
 * per mandate), replacing MSR's interim {@code collection_result} placeholder. It
 * keys collection terminal outcomes by {@code mandate_ref} - the DIRECT
 * collection -> mandate link the M10 foundation added to {@code tx_entry} - projects
 * the terminal status via {@code ext_tx_status}, and flags {@code is_terminal_failure}
 * from {@code prg_status_class.classification = 'TERMINAL_NON_SUCCESS'} (RJCT/CANC).
 *
 * <p>Proves the three-way join resolves and classifies correctly for a terminal
 * failure, a terminal success, a still-pending collection (CTV_PASS, no response
 * leg yet) and a mandate-less collection (mandate_ref NULL, excluded), and that
 * rows are keyed by mandate_ref and ordered by occurred_at.
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange",
        "DCRE_EXCHANGE_ROOT=build/test-exchange"})
class ManCollectionOutcomeIT {

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

    @Autowired
    JdbcTemplate jdbc;

    /**
     * Seed one collection line: a {@code tx_entry} carrying {@code mandateRef},
     * whose terminal outcome is projected through the {@code ext_tx_status}
     * no-emission family fallback (orgnl_msg_id = the header msg_id, no emission
     * for the arrival). {@code status} null = no response leg, so the PASS
     * validation projects as CTV_PASS (still pending). {@code ageSeconds}
     * backdates {@code tx_entry.created_at} so occurred_at ordering is
     * deterministic. A null {@code mandateRef} is a mandate-less collection.
     */
    void collection(String client, String msgId, String e2e, String mandateRef,
                    String status, int ageSeconds) {
        UUID arrival = UUID.randomUUID();
        jdbc.update("INSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count,"
                        + " initg_pty, business_date, client_token, layout_version) VALUES (?,?,?,?,?,?,?,?,?)",
                arrival, msgId, msgId, "20260716080000", 1, client, "20260716", client, 2);
        jdbc.update("INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e,"
                        + " creditor_account, mandate_ref, currency, amount_raw, amount)"
                        + " VALUES (?,1,'DC',?,?,'62000000010',?,'ZAR','1000',10.00)",
                arrival, e2e, e2e, mandateRef);
        jdbc.update("INSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,1,'PASS')", arrival);
        if (status != null) {
            jdbc.update("INSERT INTO pbsr_resp (response_file, orgnl_msg_id, e2e, status) VALUES (?,?,?,?)",
                    "RESP_" + e2e + ".xml", msgId, e2e, status);
        }
        jdbc.update("UPDATE tx_entry SET created_at = now() - INTERVAL '" + ageSeconds
                + " seconds' WHERE arrival_id = ?", arrival);
    }

    List<Map<String, Object>> byMandate(String mandateRef) {
        return jdbc.queryForList(
                "SELECT e2e, status, is_terminal_failure, occurred_at FROM man_collection_outcome "
                        + "WHERE mandate_ref = ? ORDER BY occurred_at DESC", mandateRef);
    }

    @Test
    void viewClassifiesTerminalFailureSuccessAndPendingKeyedByMandate() {
        // MND_FAIL: three terminal collections, newest first RJCT then CANC then ACSC.
        collection("FNBRF01", "MSGF1", "E2EF1", "MND_FAIL", "RJCT", 30);
        collection("FNBRF01", "MSGF2", "E2EF2", "MND_FAIL", "CANC", 60);
        collection("FNBRF01", "MSGF3", "E2EF3", "MND_FAIL", "ACSC", 90); // terminal SUCCESS, not a failure
        // MND_MIX: a single still-pending collection (CTV_PASS: no response leg yet).
        collection("FNBRF01", "MSGM1", "E2EM1", "MND_MIX", null, 10);
        // A collection with no mandate link (mandate_ref NULL) never enters the view.
        collection("FNBRF01", "MSGN1", "E2EN1", null, "RJCT", 10);

        List<Map<String, Object>> fail = byMandate("MND_FAIL");
        assertThat(fail).hasSize(3);
        assertThat(fail.get(0)).containsEntry("status", "RJCT").containsEntry("is_terminal_failure", true);
        assertThat(fail.get(1)).containsEntry("status", "CANC").containsEntry("is_terminal_failure", true);
        assertThat(fail.get(2)).containsEntry("status", "ACSC").containsEntry("is_terminal_failure", false);
        assertThat(fail.get(0).get("occurred_at")).isNotNull();

        List<Map<String, Object>> mix = byMandate("MND_MIX");
        assertThat(mix).hasSize(1);
        assertThat(mix.get(0)).containsEntry("status", "CTV_PASS").containsEntry("is_terminal_failure", false);

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM man_collection_outcome WHERE e2e = 'E2EN1'", Long.class)).isZero();
    }
}
