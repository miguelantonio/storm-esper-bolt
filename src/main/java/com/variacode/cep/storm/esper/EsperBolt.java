package com.variacode.cep.storm.esper;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 *
 */
public class EsperBolt extends BaseRichBolt implements UpdateListener {

    private static final long serialVersionUID = 1L;

    private OutputCollector collector;
    private transient EPServiceProvider epService;
    private Map<String, List<String>> outputTypes;
    private Map<String, Object> eventTypes;
    private Set<String> statements;
    private Set<EPStatementObjectModel> objectStatements;

    /**
     *
     * @param types
     * @return
     */
    public EsperBolt addOutputTypes(Map<String, List<String>> types) {
        String error = "Output Types cannot be null";
        this.outputTypes = Collections.unmodifiableMap(exceptionIfNull(error, types));
        exceptionIfAnyNull(error, types.values());
        for (List<String> s : types.values()) {
            exceptionIfAnyNull(error, s);
        }
        exceptionIfAnyNull(error, types.keySet());
        return this;
    }

    /**
     *
     * @param types
     * @return
     */
    public EsperBolt addEventTypes(Map<String, Object> types) {
        String error = "Event types cannot be null";
        this.eventTypes = Collections.unmodifiableMap(exceptionIfNull(error, types));
        exceptionIfAnyNull(error, types.values());
        exceptionIfAnyNull(error, types.keySet());
        return this;
    }

    /**
     *
     * @param statements
     * @return
     */
    public EsperBolt addStatements(Set<String> statements) {
        String error = "Statements cannot be null";
        this.statements = Collections.unmodifiableSet(exceptionIfNull(error, statements));
        exceptionIfAnyNull(error, statements);
        return this;
    }

    /**
     *
     * @param objectStatements
     * @return
     */
    public EsperBolt addObjectStatemens(Set<EPStatementObjectModel> objectStatements) {
        final String error = "Object Statements cannot be null";
        this.objectStatements = Collections.unmodifiableSet(exceptionIfNull(error, objectStatements));
        exceptionIfAnyNull(error, objectStatements);
        return this;
    }

    private <O> O exceptionIfNull(String msg, O obj) {
        exceptionIfAnyNull(msg, obj);
        return obj;
    }

    private <O> void exceptionIfAnyNull(String msg, O... obj) {
        for (O o : obj) {
            if (o == null) {
                throw new FailedException(msg);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param ofd
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        if (this.outputTypes == null) {
            throw new FailedException("outputTypes cannot be null");
        }
        for (Map.Entry<String, List<String>> outputEventType : this.outputTypes.entrySet()) {
            List<String> fields = new ArrayList<>();
            if (outputEventType.getValue() != null) {
                for (String f : outputEventType.getValue()) {
                    fields.add(f);
                }
            } else {
                throw new FailedException();
            }
            ofd.declareStream(outputEventType.getKey(), new Fields(fields));
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param map
     * @param tc
     * @param oc
     */
    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext tc, OutputCollector oc) {
        this.collector = oc;
        Configuration cepConfig = new Configuration();
        if (this.eventTypes == null || (this.objectStatements == null && this.statements == null)) {
            throw new FailedException("Event types cannot be null and at least one type of statement has to be not null");
        }
        for (Map.Entry<GlobalStreamId, Grouping> a : tc.getThisSources().entrySet()) {
            Fields f = tc.getComponentOutputFields(a.getKey());
            if (!this.eventTypes.keySet().containsAll(f.toList())) {
                throw new FailedException("Event types and fields from source streams do not match: Event Types="
                        + Arrays.toString(this.eventTypes.keySet().toArray())
                        + " Stream Fields=" + Arrays.toString(f.toList().toArray()));
            }
            cepConfig.addEventType(a.getKey().get_componentId() + "_" + a.getKey().get_streamId(), this.eventTypes);
        }
        this.epService = EPServiceProviderManager.getDefaultProvider(cepConfig);
        this.epService.initialize();
        if (!processStatemens()) {
            throw new FailedException("At least one type of statement has to be not empty");
        }
    }

    private boolean processStatemens() {
        boolean hasStatemens = false;
        if (this.statements != null) {
            for (String s : this.statements) {
                this.epService.getEPAdministrator().createEPL(s).addListener(this);
                hasStatemens = true;
            }
        }
        if (this.objectStatements != null) {
            for (EPStatementObjectModel s : this.objectStatements) {
                this.epService.getEPAdministrator().create(s).addListener(this);
                hasStatemens = true;
            }
        }
        return hasStatemens;
    }

    /**
     * {@inheritDoc}
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        Map<String, Object> tuplesper = new HashMap<>();
        for (String f : tuple.getFields()) {
            tuplesper.put(f, tuple.getValueByField(f));
        }
        this.epService.getEPRuntime().sendEvent(tuplesper, tuple.getSourceComponent() + "_" + tuple.getSourceStreamId());
        collector.ack(tuple);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
        if (this.epService != null) {
            this.epService.destroy();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param newEvents
     * @param oldEvents
     */
    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents == null) {
            return;
        }
        for (EventBean newEvent : newEvents) {
            if (outputTypes.containsKey(newEvent.getEventType().getName())) {
                List<Object> tuple = new ArrayList<>(outputTypes.get(newEvent.getEventType().getName()).size());
                for (String field : outputTypes.get(newEvent.getEventType().getName())) {
                    tuple.add(newEvent.get(field));
                }
                collector.emit(newEvent.getEventType().getName(), tuple);
            }
        }
    }

}
