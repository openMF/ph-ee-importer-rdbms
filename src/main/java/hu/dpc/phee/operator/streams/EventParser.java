package hu.dpc.phee.operator.streams;

import com.jayway.jsonpath.DocumentContext;

import java.util.Collection;

public interface EventParser {

    boolean isAbleToProcess(Collection<DocumentContext> parsedRecords);

    void process(Collection<DocumentContext> parsedRecords);
}