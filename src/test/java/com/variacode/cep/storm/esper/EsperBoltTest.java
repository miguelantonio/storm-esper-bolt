package com.variacode.cep.storm.esper;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

public class EsperBoltTest {

    /**
     * Test of execute method, of class EsperBolt.
     */
    @Test
    public void testExecute() {

        Map<String, Class> eventTypes = new HashMap<>();//should say fieldsTypes, maybe with object/component prefix
        eventTypes.put("symbol", String.class);
        eventTypes.put("price", Integer.class);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("quotes", new RandomSentenceSpout());
        builder.setBolt("esper", (new EsperBolt())
                .addEventTypes(eventTypes)
                .addOutputTypes(Collections.singletonMap("result", Arrays.asList("avg", "price")))
                .addStatements(Collections.singleton("select avg(price) as avg, price from "
                                + "quotes_default(symbol='A').win:length(2) "
                                + "having avg(price) > 3.0")))
                .shuffleGrouping("quotes");

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        //try {
        cluster.submitTopology("test", conf, builder.createTopology());
        /*} catch (RuntimeException e) {
         System.out.println("");
         }*/
        Utils.sleep(20000);
        cluster.shutdown();

        fail("The test case is a prototype.");
    }

    public static class RandomSentenceSpout extends BaseRichSpout {

        transient SpoutOutputCollector _collector;
        transient int i;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(500);
            String[] sentences = new String[]{"A", "B"};
            int[] prices = new int[]{4, 2, 1, 6, 7, 4, 6, 4, 2, 4, 4, 3, 2, 4, 4, 5, 5, 6, 6, 4, 3, 4, 4};
            String stnc = sentences[i % sentences.length];
            int prc = prices[i % prices.length];
            /*
             Object event = new Object() {
             private final String symbol;
             private final int price;

             {
             this.symbol = stnc;
             this.price = prc;
             }
             public int getPrice(){
             return price;
             }
             public String getSentence() {
             return symbol;
             }
             };
             */
            i++;
            _collector.emit(new Values(stnc, prc));
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
