package za.co.fnb.dcre.prg.data.repo;

import org.springframework.data.repository.CrudRepository;
import za.co.fnb.dcre.prg.data.model.PrgReportEntity;

import java.util.List;
import java.util.UUID;

/** SCRUM-55 report registry: every emitted report artifact, replay-addressable by id. */
public interface PrgReportRepo extends CrudRepository<PrgReportEntity, UUID> {

    List<PrgReportEntity> findByClientAndParentSourceMsgId(String client, String parentSourceMsgId);
}
