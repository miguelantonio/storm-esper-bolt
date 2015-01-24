package com.variacode.cep.storm.esper;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import com.espertech.esper.client.soda.Expressions;
import com.espertech.esper.client.soda.Filter;
import com.espertech.esper.client.soda.FilterStream;
import com.espertech.esper.client.soda.FromClause;
import com.espertech.esper.client.soda.InsertIntoClause;
import com.espertech.esper.client.soda.SelectClause;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.Test;
import static org.junit.Assert.*;

public class EsperBoltTest {

    private static Map<Integer, Double> resultEPL = new HashMap<>();
    private static Map<Integer, Double> resultSODA = new HashMap<>();

    /**
     * Test of execute method, of class EsperBolt.
     */
    @Test
    public void testSODA() {
        resultSODA = new HashMap<>();

        System.out.println("EngineTest-SODA");

        Map<String, Object> eventTypes = new HashMap<>();//should say fieldsTypes, maybe with object/component prefix
        eventTypes.put("symbol", String.class);
        eventTypes.put("price", Integer.class);

        
        EPStatementObjectModel model = new EPStatementObjectModel();
        model.setInsertInto(InsertIntoClause.create("Result"));
        model.setSelectClause(SelectClause.create()
                .add(Expressions.avg("price"),"avg")
                .add("price")
        );
        Filter filter = Filter.create("quotes_default", Expressions.eq("symbol", "A"));
        model.setFromClause(FromClause.create(
                FilterStream.create(filter)
                .addView("win","length", Expressions.constant(2))
        ));
        model.setHavingClause(Expressions.gt(Expressions.avg("price"), Expressions.constant(60.0)));
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("quotes", new RandomSentenceSpout());
        builder.setBolt("esper", (new EsperBolt())
                .addEventTypes(eventTypes)
                .addOutputTypes(Collections.singletonMap("Result", Arrays.asList("avg", "price")))
                .addObjectStatemens(Collections.singleton(model)))
                .shuffleGrouping("quotes");
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("esper", "Result");

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
        assertEquals(resultSODA.get(100), new Double(75.0));
        assertEquals(resultSODA.get(50), new Double(75.0));
    }

    /**
     * Test of execute method, of class EsperBolt.
     */
    @Test
    public void testEPL() {
        resultEPL = new HashMap<>();

        System.out.println("EngineTest-EPL");

        Map<String, Object> eventTypes = new HashMap<>();//should say fieldsTypes, maybe with object/component prefix
        eventTypes.put("symbol", String.class);
        eventTypes.put("price", Integer.class);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("quotes", new RandomSentenceSpout());
        builder.setBolt("esper", (new EsperBolt())
                .addEventTypes(eventTypes)
                .addOutputTypes(Collections.singletonMap("Result", Arrays.asList("avg", "price")))
                .addStatements(Collections.singleton("insert into Result "
                                + "select avg(price) as avg, price from "
                                + "quotes_default(symbol='A').win:length(2) "
                                + "having avg(price) > 60.0")))
                .shuffleGrouping("quotes");
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("esper", "Result");

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
        assertEquals(resultEPL.get(100), new Double(75.0));
        assertEquals(resultEPL.get(50), new Double(75.0));
    }

    public static class PrinterBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            resultEPL.put(tuple.getIntegerByField("price"), tuple.getDoubleByField("avg"));
            resultSODA.put(tuple.getIntegerByField("price"), tuple.getDoubleByField("avg"));
            System.out.println(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
        }

    }

    public static class RandomSentenceSpout extends BaseRichSpout {

        transient ConcurrentLinkedQueue<HashMap.SimpleEntry<String, Integer>> data;
        transient SpoutOutputCollector _collector;
        transient int i;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            data = new ConcurrentLinkedQueue<>();
            data.add(new HashMap.SimpleEntry<>("A", 50));
            data.add(new HashMap.SimpleEntry<>("A", 100));
            data.add(new HashMap.SimpleEntry<>("A", 50));
            data.add(new HashMap.SimpleEntry<>("B", 50));
            data.add(new HashMap.SimpleEntry<>("A", 30));
            data.add(new HashMap.SimpleEntry<>("C", 50));
            data.add(new HashMap.SimpleEntry<>("A", 50));
        }

        @Override
        public void nextTuple() {
            Utils.sleep(500);
            HashMap.SimpleEntry<String, Integer> d = this.data.poll();
            if (d != null) {
                System.out.println("EMITED: " + d.getKey() + " " + d.getValue());
                _collector.emit(new Values(d.getKey(), d.getValue()));
            }

        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("symbol", "price"));
        }

    }

}
