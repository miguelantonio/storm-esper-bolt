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
import com.espertech.esper.client.soda.EPStatementObjectModel;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import static org.junit.Assert.*;

public class AbstractEsperBoltTest {

    /**
     * Test of execute method, of class AbstractEsperBolt.
     */
    @Test
    public void testExecute() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new RandomSentenceSpout());
        builder.setBolt("esper", new AbstractEsperBoltImpl())
                .shuffleGrouping("word");

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, builder.createTopology());

        Utils.sleep(20000);
        cluster.shutdown();

        /*System.out.println("execute");
         Tuple tuple = null;
         AbstractEsperBolt instance = new AbstractEsperBoltImpl();
         instance.execute(tuple);*/
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    public class AbstractEsperBoltImpl extends AbstractEsperBolt {

        public Map<String,Class> getEventTypes() {
            return null;
        }

        public Set<String> getEPLStatements() {
            return null;
        }

        public Set<EPStatementObjectModel> getEPObjectStatements() {
            return null;
        }
    }

    public class RandomSentenceSpout extends BaseRichSpout {

        SpoutOutputCollector _collector;
        int i = 0;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(500);
            String[] sentences = new String[]{"the cow jumped over the moon", "an apple a day keeps the doctor away",
                "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature"};
            final String stnc = sentences[i%sentences.length];
            
            Object event = new Object(){
                private final String sentence;
                {
                    this.sentence = stnc;
                }
                public String getSentence(){
                    return sentence;
                }
            };
            
            i++;
            _collector.emit(new Values(event));
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }

}
