package za.co.fnb.dcre.prg.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.prg.data.model.PrgWatermarkEntity;
import za.co.fnb.dcre.prg.data.model.StatusRow;
import za.co.fnb.dcre.prg.data.model.UnknownRow;

import java.util.List;
import java.util.UUID;

public interface PrgWatermarkRepo extends CrudRepository<PrgWatermarkEntity, UUID> {

    /**
     * Bounded keyset slice of the delta: rows whose external status moved past
     * the client's watermark (SCRUM-42 load fix: the whole-book variant blew
     * CRDB's sql memory budget on the 30M-tx book; resume after the caller's
     * last e2e, '' for the first slice). status IS NOT NULL: a window can fire
     * mid-DAG before CTV verdicts exist; an unknown status is not reportable
     * (and would violate the watermark NOT NULL).
     */
    @Query(value = """
            SELECT x.e2e, x.status
            FROM ext_tx_status x
            LEFT JOIN prg_watermark w ON w.client = x.client AND w.e2e = x.e2e
            WHERE x.client = :client AND x.status IS NOT NULL
              AND (w.e2e IS NULL OR w.last_status <> x.status)
              AND x.e2e > :afterE2e
            ORDER BY x.e2e
            LIMIT :limit""", rowMapperClass = StatusRowMapper.class)
    List<StatusRow> findDeltaSlice(@Param("client") String client, @Param("afterE2e") String afterE2e,
                                   @Param("limit") int limit);

    /** Resend override, same keyset slicing: ALL current known-status rows, watermark ignored. */
    @Query(value = """
            SELECT x.e2e, x.status
            FROM ext_tx_status x
            WHERE x.client = :client AND x.status IS NOT NULL
              AND x.e2e > :afterE2e
            ORDER BY x.e2e
            LIMIT :limit""", rowMapperClass = StatusRowMapper.class)
    List<StatusRow> findRangeSlice(@Param("client") String client, @Param("afterE2e") String afterE2e,
                                   @Param("limit") int limit);

    /**
     * Aggregate count of mid-DAG rows with no reportable status (R-38 at
     * scale): the row-returning whole-book scan died live with
     * "sql: memory budget exceeded" on the 30M-tx book.
     */
    @Query("SELECT count(*) FROM ext_tx_status x WHERE x.client = :client AND x.status IS NULL")
    long countUnknown(@Param("client") String client);

    /** Bounded per-row detail for small unknown counts: keeps the uniform R-38 WARN shape below the limit. */
    @Query(value = """
            SELECT x.arrival_id, x.sequence, x.e2e
            FROM ext_tx_status x
            WHERE x.client = :client AND x.status IS NULL
            ORDER BY x.arrival_id, x.sequence
            LIMIT :limit""", rowMapperClass = UnknownRowMapper.class)
    List<UnknownRow> findUnknownDetail(@Param("client") String client, @Param("limit") int limit);

    /** NEVER UPSERT INTO: CRDB resolves UPSERT on PK only; business identity is (client, e2e). */
    @Modifying
    @Query("""
            INSERT INTO prg_watermark (id, client, e2e, last_status)
            VALUES (gen_random_uuid(), :client, :e2e, :status)
            ON CONFLICT (client, e2e)
            DO UPDATE SET last_status = excluded.last_status, updated_at = now()""")
    void upsertWatermark(@Param("client") String client, @Param("e2e") String e2e,
                         @Param("status") String status);
}
