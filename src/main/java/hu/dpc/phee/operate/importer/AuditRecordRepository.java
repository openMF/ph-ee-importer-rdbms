package hu.dpc.phee.operate.importer;

import hu.dpc.phee.operate.importer.entity.AuditRecord;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;

@Repository
@Transactional
public interface AuditRecordRepository extends CrudRepository<AuditRecord, Long> {

}
