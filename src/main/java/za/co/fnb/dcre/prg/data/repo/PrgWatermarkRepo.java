package za.co.fnb.dcre.prg.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.prg.data.model.PrgWatermarkEntity;
import za.co.fnb.dcre.prg.data.model.StatusRow;

import java.util.List;
import java.util.UUID;

public interface PrgWatermarkRepo extends CrudRepository<PrgWatermarkEntity, UUID> {

    /**
     * Delta selection: rows whose external status moved past the client's watermark.
     * status IS NOT NULL: a window can fire mid-DAG before CTV verdicts exist; an
     * unknown status is not reportable (and would violate the watermark NOT NULL).
     */
    @Query(value = """
            SELECT x.e2e, x.status
            FROM ext_tx_status x
            LEFT JOIN prg_watermark w ON w.client = x.client AND w.e2e = x.e2e
            WHERE x.client = :client AND x.status IS NOT NULL
              AND (w.e2e IS NULL OR w.last_status <> x.status)
            ORDER BY x.e2e""", rowMapperClass = StatusRowMapper.class)
    List<StatusRow> findDelta(@Param("client") String client);

    /** Resend override: ALL current known-status rows for the client, watermark ignored. */
    @Query(value = """
            SELECT x.e2e, x.status
            FROM ext_tx_status x
            WHERE x.client = :client AND x.status IS NOT NULL
            ORDER BY x.e2e""", rowMapperClass = StatusRowMapper.class)
    List<StatusRow> findRange(@Param("client") String client);

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
