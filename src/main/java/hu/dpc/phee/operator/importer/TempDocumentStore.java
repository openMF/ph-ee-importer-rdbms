package hu.dpc.phee.operator.importer;

import com.jayway.jsonpath.DocumentContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

@Component
public class TempDocumentStore {

    private final Map<Long, List<DocumentContext>> tempVariableEvents = new HashMap<>();
    private final Map<Long, String> workflowkeyBpmnAccociations = new ConcurrentHashMap<>();

    private final Map<Long, String> workflowKeyBatchFileNameAssociations = new HashMap<>();

    private final Map<Long, String> workflowKeyBatchIdAssociations = new HashMap<>();

    public String getBpmnprocessId(Long workflowKey) {
        return workflowkeyBpmnAccociations.get(workflowKey);
    }

    public void setBpmnprocessId(Long workflowKey, String bpmnprocessId) {
        workflowkeyBpmnAccociations.putIfAbsent(workflowKey, bpmnprocessId);
    }

    public void storeDocument(Long workflowKey, DocumentContext document) {
        synchronized (tempVariableEvents) {
            List<DocumentContext> existingEvents = tempVariableEvents.get(workflowKey);
            if (existingEvents == null) {
                existingEvents = new ArrayList<>();
            }
            existingEvents.add(document);
            tempVariableEvents.put(workflowKey, existingEvents);
        }
    }

    public void deleteDocument(Long workflowKey) {
        synchronized (tempVariableEvents) {
            tempVariableEvents.remove(workflowKey);
        }
        synchronized (workflowkeyBpmnAccociations) {
            workflowkeyBpmnAccociations.remove(workflowKey);
        }
    }

    public List<DocumentContext> takeStoredDocuments(Long workflowKey) {
        synchronized (tempVariableEvents) {
            List<DocumentContext> removedDocuments = tempVariableEvents.remove(workflowKey);
            return removedDocuments == null ? Collections.emptyList() : removedDocuments;
        }
    }

    public void storeBatchFileName(Long workflowKey, String filename) {
        synchronized (workflowKeyBatchFileNameAssociations) {
            workflowKeyBatchFileNameAssociations.put(workflowKey, filename);
        }
    }

    public String getBatchFileName(Long workflowKey) {
        synchronized (workflowKeyBatchFileNameAssociations) {
            return workflowKeyBatchFileNameAssociations.get(workflowKey);
        }
    }

    public void storeBatchId(Long workflowKey, String batchId) {
        synchronized (workflowKeyBatchFileNameAssociations) {
            workflowKeyBatchIdAssociations.put(workflowKey, batchId);
        }
    }

    public String getBatchId(Long workflowKey) {
        synchronized (workflowKeyBatchFileNameAssociations) {
            return workflowKeyBatchIdAssociations.get(workflowKey);
        }
    }
}
