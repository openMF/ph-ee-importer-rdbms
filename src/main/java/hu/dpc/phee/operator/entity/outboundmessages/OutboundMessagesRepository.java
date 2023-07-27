package hu.dpc.phee.operator.entity.outboundmessages;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import java.util.Optional;

public interface OutboundMessagesRepository extends JpaRepository<OutboudMessages, Long>, JpaSpecificationExecutor {
    Optional<OutboudMessages> findByInternalId(Long internalId);
}
