package hu.dpc.phee.operator.entity.filetransport;

import org.springframework.data.jpa.repository.JpaRepository;

public interface FileTransportRepository extends JpaRepository<FileTransport, Long> {
    FileTransport findByWorkflowInstanceKey(Long processInstanceKey);
}