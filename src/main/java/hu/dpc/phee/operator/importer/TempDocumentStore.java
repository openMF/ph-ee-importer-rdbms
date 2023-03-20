package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.DocumentContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TempDocumentStore {
    private final Map<Long, List<DocumentContext>> tempVariableEvents = new ConcurrentHashMap<>();
    private final Map<Long, String> workflowkeyBpmnAccociations = new ConcurrentHashMap<>();

    private final Map<Long, String> workflowKeyBatchFileNameAssociations = new ConcurrentHashMap<>();

    private final Map<Long, String> workflowKeyBatchIdAssociations = new ConcurrentHashMap<>();

    public String getBpmnprocessId(Long workflowKey) {
        return workflowkeyBpmnAccociations.get(workflowKey);
    }

    public void setBpmnprocessId(Long workflowKey, String bpmnprocessId) {
        workflowkeyBpmnAccociations.putIfAbsent(workflowKey, bpmnprocessId);
    }

    public void storeDocument(Long workflowKey, DocumentContext document) {
        List<DocumentContext> existingEvents = tempVariableEvents.get(workflowKey);
        if (existingEvents == null) {
            existingEvents = new ArrayList<>();
        }
        existingEvents.add(document);
        tempVariableEvents.put(workflowKey, existingEvents);
    }

    public void deleteDocument(Long workflowKey) {
        tempVariableEvents.remove(workflowKey);
        workflowkeyBpmnAccociations.remove(workflowKey);
    }

    public List<DocumentContext> takeStoredDocuments(Long workflowKey) {
        List<DocumentContext> removedDocuments = tempVariableEvents.remove(workflowKey);
        return removedDocuments == null ? Collections.emptyList() : removedDocuments;
    }


    public void storeBatchFileName(Long workflowKey, String filename) {
        workflowKeyBatchFileNameAssociations.put(workflowKey, filename);
    }

    public String getBatchFileName(Long workflowKey) {
        return workflowKeyBatchFileNameAssociations.get(workflowKey);
    }

    public void storeBatchId(Long workflowKey, String batchId) {
        workflowKeyBatchIdAssociations.put(workflowKey, batchId);
    }

    public String getBatchId(Long workflowKey) {
        return workflowKeyBatchIdAssociations.get(workflowKey);
    }
}
