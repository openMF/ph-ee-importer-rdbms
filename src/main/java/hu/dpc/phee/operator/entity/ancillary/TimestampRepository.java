package hu.dpc.phee.operator.entity.ancillary;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface TimestampRepository extends JpaRepository<Timestamps, String>, JpaSpecificationExecutor {

}
