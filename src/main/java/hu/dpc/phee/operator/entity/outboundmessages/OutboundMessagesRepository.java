package hu.dpc.phee.operator.entity.outboundmessages;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface OutboundMessagesRepository extends JpaRepository<OutboudMessages, Long>, JpaSpecificationExecutor {
    OutboudMessages findByInternalId(Long internalId);
}
