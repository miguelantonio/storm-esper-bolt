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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import static org.junit.Assert.*;

public class EsperBoltTest {

    /**
     * Test of execute method, of class AbstractEsperBolt.
     */
    @Test
    public void testExecute() {
        /*
       //serialize the List
    try (
      OutputStream file = new FileOutputStream("quarks.ser");
      OutputStream buffer = new BufferedOutputStream(file);
      ObjectOutput output = new ObjectOutputStream(buffer);
    ){
      output.writeObject(new AbstractEsperBolt());
    }  
    catch(IOException ex){
        System.out.println("");
    }

    //deserialize the quarks.ser file
    try(
      InputStream file = new FileInputStream("quarks.ser");
      InputStream buffer = new BufferedInputStream(file);
      ObjectInput input = new ObjectInputStream (buffer);
    ){
      //deserialize the List
      AbstractEsperBolt recoveredQuarks = (AbstractEsperBolt)input.readObject();
      //display its data
        System.out.println("");
    }
    catch(ClassNotFoundException ex){
        System.out.println("");
    }
    catch(IOException ex){
        System.out.println("");
    }
        */
        
        
        
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new RandomSentenceSpout());
        builder.setBolt("esper", (new EsperBolt()).addOutputTypes(null).addStatements(null))
                .shuffleGrouping("word");

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
try{
        cluster.submitTopology("test", conf, builder.createTopology());
}catch(RuntimeException e){
    System.out.println("");
}
        Utils.sleep(20000);
        cluster.shutdown();

        /*System.out.println("execute");
         Tuple tuple = null;
         AbstractEsperBolt instance = new AbstractEsperBoltImpl();
         instance.execute(tuple);*/
        // TODO review the generated test code and remove the default call to fail.
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
