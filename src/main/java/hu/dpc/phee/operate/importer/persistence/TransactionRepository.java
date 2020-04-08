package hu.dpc.phee.operate.importer.persistence;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;
import java.util.List;

@Repository
@Transactional
public interface TransactionRepository extends CrudRepository<Transaction, Long> {

    List<Transaction> findByBusinessKeyAndBusinessKeyType(String businessKey, String businessKeyType);

}
