/*
 * Copyright 2015 Variacode
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.Token;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

public class EsperBoltStressTest {

    private static long startMilis;
    private static long endMilis;
    private static long tuplesCount;
    private static final String[] header = {"id", "symbol", "datetime", "buyPrice", "sellPrice", "type"};
    private static final String LITERAL_RETURN_OBJ = "Result";
    private static final String LITERAL_ESPER = "esper";
    private static final String LITERAL_QUOTES = "quotes";
    private static final boolean DEBUG = true;

    private static void write(String msg) {
        Logger.getLogger(EsperBoltStressTest.class.getName()).log(Level.INFO, msg);
    }

    private static void log(String msg) {
        if (DEBUG) {
            write(msg);
        }
    }

    /**
     * Test of execute method, of class EsperBolt.
     *
     */
    @Test
    public void testEPL() {

        Logger.getLogger(EsperBoltStressTest.class.getName()).log(Level.INFO, "EngineTest-EPL");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(LITERAL_QUOTES, new SpreadSpout());
        builder.setBolt(LITERAL_ESPER, (new EsperBolt())
                .addEventTypes(ForexSpreadTestBean.class)
                .addOutputTypes(Collections.singletonMap(LITERAL_RETURN_OBJ,
                                Arrays.asList("avg", "buyPrice")))
                .addStatements(Collections.singleton("insert into Result "
                                + "select avg(buyPrice) as avg, buyPrice from "
                                + "quotes_default(symbol='AUD/USD').win:length(2) "
                                + "having avg(buyPrice) > 0.0")))
                .shuffleGrouping(LITERAL_QUOTES);
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping(LITERAL_ESPER, LITERAL_RETURN_OBJ);

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        startMilis = System.currentTimeMillis();
        Utils.sleep(15000);
        cluster.shutdown();
        write("TUPLAS PROCESADAS: " + tuplesCount);
        write("MILISEGUNDOS: " + (endMilis - startMilis));
        write("VELOCIDAD: " + (tuplesCount / (endMilis - startMilis)) + " TUPLAS/MILISEGUNDO");
        assertEquals(true, true);
    }

    /**
     *
     */
    public static class PrinterBolt extends BaseBasicBolt {

        /**
         *
         * @param tuple
         * @param collector
         */
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            log("PRICE: " + tuple.getDoubleByField("buyPrice") + " - AVG: " + tuple.getDoubleByField("avg"));
            endMilis = System.currentTimeMillis();
        }

        /**
         *
         * @param ofd
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
            //Not implemented
        }

    }

    /**
     *
     */
    public static class SpreadSpout extends BaseRichSpout {

        transient SpoutOutputCollector collector;
        transient CellProcessor[] processors;
        transient ICsvBeanReader beanReader;

        /**
         *
         * @param conf
         * @param context
         * @param collector
         */
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            CellProcessor[] p = {new Optional(), new Optional(),
                new Optional(new Token(" ", null, new ParseDate("yyyy-MM-dd HH:mm:ss.SSS", true))),
                new Optional(new ParseDouble()), new Optional(new ParseDouble()), new Optional()};
            this.processors = p;
            try {
                this.beanReader = new CsvBeanReader(new FileReader("src/test/resources/AUD_USD_Week1.CSV"), CsvPreference.STANDARD_PREFERENCE);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(EsperBoltStressTest.class.getName()).log(Level.SEVERE, null, ex);
                this.beanReader = null;
            }
        }

        /**
         *
         */
        @Override
        public void nextTuple() {
            if (this.beanReader != null) {
                tuplesCount++;
                ForexSpreadTestBean spread;
                try {
                    if ((spread = beanReader.read(ForexSpreadTestBean.class, EsperBoltStressTest.header, processors)) != null) {
                        log(String.format("lineNo=%s, rowNo=%s, spread=%s", beanReader.getLineNumber(),
                                beanReader.getRowNumber(), spread));
                        this.collector.emit(new Values(spread.getId(), spread.getSymbol(), spread.getDatetime(), spread.getBuyPrice(), spread.getSellPrice(), spread.getType()));
                    }
                } catch (IOException ex) {
                    Logger.getLogger(EsperBoltStressTest.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        /**
         *
         * @param id
         */
        @Override
        public void ack(Object id) {
            //Not implemented
        }

        /**
         *
         * @param id
         */
        @Override
        public void fail(Object id) {
            //Not implemented
        }

        /**
         *
         * @param declarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(EsperBoltStressTest.header));
        }

    }

}
