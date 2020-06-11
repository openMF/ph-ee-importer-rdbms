package hu.dpc.phee.operator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "bpmn")
public class BpmnProcessProperties {

    private List<BpmnProcess> processes = new ArrayList<>();

    public BpmnProcessProperties() {
    }

    public List<BpmnProcess> getProcesses() {
        return processes;
    }

    public void setProcesses(List<BpmnProcess> processes) {
        this.processes = processes;
    }

    public BpmnProcess getById(String bpmnProcessId) {
        return getProcesses().stream()
                .filter(p -> p.getId().equals(bpmnProcessId))
                .findFirst()
                .orElse(new BpmnProcess("UNKNOWN"));
    }
}
