package com.variacode.cep.storm.esper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractEsperBolt extends BaseRichBolt {

    private transient OutputCollector collector;
    private transient EPServiceProvider epService;
    private Map<String,Map<String,Object>> types;
    
    public AbstractEsperBolt outputTypes(Map<String,Map<String,Object>> types){
        this.types = types;//TODO: make sure the user cannot change this at will or problems
        return this;
    }
    
    protected abstract Set<String> getEPLStatements();

    protected abstract Set<EPStatementObjectModel> getEPObjectStatements();

    private final UpdateListener eventListener = new UpdateListener() {

        @Override
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null) {
                for (EventBean newEvent : newEvents) {
                    if(types.containsKey(newEvent.getEventType().getName())){
                        collector.emit(newEvent.getEventType().getName(), null);
                    }
                        
                    Class eventType = getEventTypes().get(newEvent.getEventType().getName());
                    if (eventType != null) {
                        List<Object> tuple = new ArrayList<>(eventType.getDeclaredFields().length);
                        for (Field f : eventType.getDeclaredFields()) {
                            tuple.add(newEvent.get(f.getName()));
                        }
                        collector.emit(newEvent.getEventType().getName(), tuple);
                    }
                }
            }
        }

    };

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        
        for (Map.Entry<String, Class> event : getEventTypes().entrySet()) {
            List<String> fields = new ArrayList<>();
            for (Field f : event.getValue().getFields()) {
                fields.add(f.getName());
            }
            ofd.declareStream(event.getKey(), new Fields(fields));
        }
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext tc, OutputCollector oc) {
        this.collector = oc;
        Configuration cepConfig = new Configuration();
        for (Map.Entry<String, Class> event : getEventTypes().entrySet()) {
            cepConfig.addEventType(event.getKey(), event.getClass().getName());
        }
        this.epService = EPServiceProviderManager.getDefaultProvider(cepConfig);
        this.epService.initialize();
        if (getEPLStatements() != null) {
            for (String s : getEPLStatements()) {
                this.epService.getEPAdministrator().createEPL(s).addListener(this.eventListener);
            }
        }
        if (getEPObjectStatements() != null) {
            for (EPStatementObjectModel s : getEPObjectStatements()) {
                this.epService.getEPAdministrator().create(s).addListener(this.eventListener);
            }
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Map<String, Object> tuplesper = new HashMap<>();
        for(String f : tuple.getFields()){
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

}
