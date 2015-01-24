package com.variacode.cep.storm.esper;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EsperBolt extends BaseRichBolt implements UpdateListener {

    private static final long serialVersionUID = 1L;

    private OutputCollector collector;
    private transient EPServiceProvider epService;
    private Map<String, List<String>> outputTypes;
    private Map<String, Class> eventTypes;
    private Set<String> statements;
    private Set<EPStatementObjectModel> objectStatements;

    public EsperBolt addOutputTypes(Map<String, List<String>> types) {
        //TODO: make sure the user cannot change this because not
        this.outputTypes = Collections.unmodifiableMap(exceptionIfNull(types));
        return this;
    }

    public EsperBolt addEventTypes(Map<String, Class> types) {
        this.eventTypes = Collections.unmodifiableMap(exceptionIfNull(types));
        return this;
    }

    public EsperBolt addStatements(Set<String> statements) {
        this.statements = Collections.unmodifiableSet(exceptionIfNull(statements));
        return this;
    }

    public EsperBolt addObjectStatemens(Set<EPStatementObjectModel> objectStatements) {
        this.objectStatements = Collections.unmodifiableSet(exceptionIfNull(objectStatements));
        return this;
    }

    private <O> O exceptionIfNull(O obj) {
        if (obj == null) {
            throw new RuntimeException();//TODO fix
        }
        return obj;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        if (this.outputTypes == null) {
            throw new RuntimeException();//FUCK YOU (and please help me make another Exception)
        }
        for (Map.Entry<String, List<String>> outputEventType : this.outputTypes.entrySet()) {
            List<String> fields = new ArrayList<>();
            if (outputEventType.getValue() != null) {
                for (String f : outputEventType.getValue()) {
                    fields.add(f);
                }
            } else {
                throw new RuntimeException();//NO
            }
            ofd.declareStream(outputEventType.getKey(), new Fields(fields));//TODO: ver si usar default
        }
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext tc, OutputCollector oc) {
        this.collector = oc;
        Configuration cepConfig = new Configuration();
        if (this.eventTypes == null || (this.objectStatements == null && this.statements == null)) {
            throw new RuntimeException();//TODO: FUCK YOU (fix)
        }
        for (Map.Entry<GlobalStreamId, Grouping> a : tc.getThisSources().entrySet()) {
            Fields f = tc.getComponentOutputFields(a.getKey());
            if (!this.eventTypes.keySet().containsAll(f.toList())) {
                throw new RuntimeException();//TODO: please die..
            }
            String[] flds_pls = new String[this.eventTypes.size()];
            this.eventTypes.keySet().toArray(flds_pls);
            Class[] clss_pls = new Class[this.eventTypes.size()];
            this.eventTypes.values().toArray(clss_pls);
            cepConfig.addEventType(a.getKey().get_componentId() + "_" + a.getKey().get_streamId(), flds_pls, clss_pls);
        }
        this.epService = EPServiceProviderManager.getDefaultProvider(cepConfig);
        this.epService.initialize();
        if (this.statements != null) {
            for (String s : this.statements) {
                this.epService.getEPAdministrator().createEPL(s).addListener(this);
            }
        }
        if (this.objectStatements != null) {
            for (EPStatementObjectModel s : this.objectStatements) {
                this.epService.getEPAdministrator().create(s).addListener(this);
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Map<String, Object> tuplesper = new HashMap<>();
        for (String f : tuple.getFields()) {
            tuplesper.put(f, tuple.getValueByField(f));
        }
        this.epService.getEPRuntime().sendEvent(tuplesper, tuple.getSourceStreamId());
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        if (this.epService != null) {
            this.epService.destroy();
        }
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents != null) {
            for (EventBean newEvent : newEvents) {
                if (outputTypes.containsKey(newEvent.getEventType().getName())) {
                    List<Object> tuple = new ArrayList<>(outputTypes.get(newEvent.getEventType().getName()).size());
                    for (String field : outputTypes.get(newEvent.getEventType().getName())) {
                        tuple.add(newEvent.get(field));
                    }
                    collector.emit(newEvent.getEventType().getName(), tuple);
                }//TODO: think about what to do with your life here
            }
        }
    }

}
