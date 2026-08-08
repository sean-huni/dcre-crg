package za.co.fnb.dcre.crg.data.repo;

import org.springframework.data.repository.CrudRepository;
import za.co.fnb.dcre.crg.data.model.CrgReportEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** SCRUM-55 report registry: every emitted report artifact, replay-addressable by id. */
public interface CrgReportRepo extends CrudRepository<CrgReportEntity, UUID> {

    List<CrgReportEntity> findByClientAndParentSourceMsgId(String client, String parentSourceMsgId);

    /** file_name is unique: the restart no-op lookup (a standing row means the emission already registered). */
    Optional<CrgReportEntity> findByFileName(String fileName);

    /**
     * SCRUM-58 clock-scoped trace: every PSR artifact emitted under one batch
     * execution, addressable by its seam job_name (JOB_NAME env or
     * {@code local-crg-<executionId>}); a supporter holding the seam name
     * resolves the reports without needing an arrival.
     */
    List<CrgReportEntity> findByJobName(String jobName);
}
