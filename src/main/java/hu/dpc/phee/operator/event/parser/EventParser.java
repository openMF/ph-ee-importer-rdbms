package hu.dpc.phee.operator.event.parser;

import hu.dpc.phee.operator.event.parser.impl.EventRecord;
import org.springframework.beans.factory.NamedBean;

import java.util.List;

public interface EventParser extends NamedBean {

    boolean isAbleToProcess(List<EventRecord> eventRecords);

    void process(List<EventRecord> eventRecords);
}