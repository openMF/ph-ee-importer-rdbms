package hu.dpc.phee.operator.entity.businesskey;

import java.util.List;
import org.springframework.data.repository.CrudRepository;

public interface BusinessKeyRepository extends CrudRepository<BusinessKey, Long> {

    List<BusinessKey> findByBusinessKeyAndBusinessKeyType(String businessKey, String businessKeyType);

}
