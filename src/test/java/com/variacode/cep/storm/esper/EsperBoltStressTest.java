/*
 * Copyright 2015 Variacode
 *
 * Licensed under the GPLv2 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.variacode.cep.storm.esper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.Token;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

public class EsperBoltStressTest {

    private static volatile long startMilis;
    private static volatile long endMilis;
    private static final AtomicLong tuplesCount = new AtomicLong(0);
    private static final AtomicLong matchedCount = new AtomicLong(0);
    private static final String[] header = {"id", "symbol", "datetime", "buyPrice", "sellPrice", "type"};
    private static final String LITERAL_RETURN_OBJ = "Result";
    private static final String LITERAL_ESPER = "esper";
    private static final String LITERAL_QUOTES = "quotes";
    private static final boolean DEBUG = false;
    private static final int MAX_INPUT_FILES = 1;

    private static void write(String msg) {
        Logger.getLogger(EsperBoltStressTest.class.getName()).log(Level.INFO, msg);
    }

    private static void log(String msg) {
        if (DEBUG) {
            write(msg);
        }
    }

    private static CellProcessor[] createProcessors() {
        return new CellProcessor[]{
            new Optional(),
            new Optional(),
            new Optional(new Token(" ", null, new ParseDate("yyyy-MM-dd HH:mm:ss.SSS", true))),
            new Optional(new ParseDouble()),
            new Optional(new ParseDouble()),
            new Optional()
        };
    }

    /**
     * High-throughput stress test feeding all 96,117 CSV events through EsperBolt.
     */
    @Test
    public void testDirectHighThroughputStress() throws Exception {
        AtomicLong acked = new AtomicLong(0);
        AtomicLong emitted = new AtomicLong(0);
        AtomicLong errors = new AtomicLong(0);

        EsperBolt bolt = new EsperBolt()
                .addEventTypes(ForexSpreadTestBean.class)
                .addOutputTypes(Collections.singletonMap(LITERAL_RETURN_OBJ,
                        Arrays.asList("avg", "buyPrice")))
                .addStatements(Collections.singleton("insert into Result "
                        + "select avg(buyPrice) as avg, buyPrice from "
                        + "quotes_default(symbol='AUD/USD').win:length(2) "
                        + "having avg(buyPrice) > 0.76"));

        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                emitted.incrementAndGet();
                return Collections.emptyList();
            }

            @Override
            public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            }

            @Override
            public void ack(Tuple input) {
                acked.incrementAndGet();
            }

            @Override
            public void fail(Tuple input) {
                errors.incrementAndGet();
            }

            @Override
            public void resetTimeout(Tuple input) {
            }

            @Override
            public void flush() {
            }

            @Override
            public void reportError(Throwable error) {
                errors.incrementAndGet();
            }
        });

        GlobalStreamId streamId = new GlobalStreamId(LITERAL_QUOTES, Utils.DEFAULT_STREAM_ID);
        Fields fields = new Fields(header);
        TopologyContext context = new TopologyContext(
                null, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(),
                null, null, null, 1, 1, Collections.singletonList(1),
                new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(),
                new java.util.concurrent.atomic.AtomicBoolean(false), null) {
            @Override
            public Map<GlobalStreamId, Grouping> getThisSources() {
                return Collections.singletonMap(streamId, new Grouping());
            }

            @Override
            public Fields getComponentOutputFields(GlobalStreamId id) {
                return fields;
            }

            @Override
            public String getThisComponentId() {
                return LITERAL_ESPER;
            }

            @Override
            public int getThisTaskId() {
                return 1;
            }
        };

        bolt.prepare(new HashMap<>(), context, collector);

        long start = System.currentTimeMillis();
        long rowCount = 0;
        CellProcessor[] processors = createProcessors();
        try (ICsvBeanReader reader = new CsvBeanReader(
                new FileReader("src/test/resources/AUD_USD_Week1.CSV"),
                CsvPreference.STANDARD_PREFERENCE)) {
            ForexSpreadTestBean spread;
            while ((spread = reader.read(ForexSpreadTestBean.class, header, processors)) != null) {
                rowCount++;
                final ForexSpreadTestBean current = spread;
                Tuple tuple = createTestTuple(fields, current);
                bolt.execute(tuple);
            }
        } finally {
            bolt.cleanup();
        }
        long elapsed = Math.max(1L, System.currentTimeMillis() - start);
        write("DIRECT STRESS TUPLAS PROCESADAS: " + acked.get() + " (EMITIDAS: " + emitted.get() + ")");
        write("MILISEGUNDOS: " + elapsed);
        write("VELOCIDAD: " + (acked.get() / elapsed) + " TUPLAS/MILISEGUNDO");

        assertEquals(96117L, rowCount);
        assertEquals(rowCount, acked.get());
        assertEquals(0L, errors.get());
        assertTrue(emitted.get() > 80000L, "Expected > 80,000 matching events emitted from EsperBolt");
    }

    private static Tuple createTestTuple(Fields fields, ForexSpreadTestBean spread) {
        Map<String, Object> values = new HashMap<>();
        values.put("id", spread.getId());
        values.put("symbol", spread.getSymbol());
        values.put("datetime", spread.getDatetime());
        values.put("buyPrice", spread.getBuyPrice());
        values.put("sellPrice", spread.getSellPrice());
        values.put("type", spread.getType());
        return new Tuple() {
            @Override
            public int size() {
                return fields.size();
            }

            @Override
            public boolean contains(String field) {
                return values.containsKey(field);
            }

            @Override
            public Fields getFields() {
                return fields;
            }

            @Override
            public int fieldIndex(String field) {
                return fields.fieldIndex(field);
            }

            @Override
            public List<Object> select(Fields selector) {
                return Collections.emptyList();
            }

            @Override
            public Object getValue(int i) {
                return values.get(fields.get(i));
            }

            @Override
            public String getString(int i) {
                return (String) getValue(i);
            }

            @Override
            public Integer getInteger(int i) {
                return (Integer) getValue(i);
            }

            @Override
            public Long getLong(int i) {
                return (Long) getValue(i);
            }

            @Override
            public Boolean getBoolean(int i) {
                return (Boolean) getValue(i);
            }

            @Override
            public Short getShort(int i) {
                return (Short) getValue(i);
            }

            @Override
            public Byte getByte(int i) {
                return (Byte) getValue(i);
            }

            @Override
            public Double getDouble(int i) {
                return (Double) getValue(i);
            }

            @Override
            public Float getFloat(int i) {
                return (Float) getValue(i);
            }

            @Override
            public byte[] getBinary(int i) {
                return (byte[]) getValue(i);
            }

            @Override
            public Object getValueByField(String field) {
                return values.get(field);
            }

            @Override
            public String getStringByField(String field) {
                return (String) values.get(field);
            }

            @Override
            public Integer getIntegerByField(String field) {
                return (Integer) values.get(field);
            }

            @Override
            public Long getLongByField(String field) {
                return (Long) values.get(field);
            }

            @Override
            public Boolean getBooleanByField(String field) {
                return (Boolean) values.get(field);
            }

            @Override
            public Short getShortByField(String field) {
                return (Short) values.get(field);
            }

            @Override
            public Byte getByteByField(String field) {
                return (Byte) values.get(field);
            }

            @Override
            public Double getDoubleByField(String field) {
                return (Double) values.get(field);
            }

            @Override
            public Float getFloatByField(String field) {
                return (Float) values.get(field);
            }

            @Override
            public byte[] getBinaryByField(String field) {
                return (byte[]) values.get(field);
            }

            @Override
            public List<Object> getValues() {
                return Arrays.asList(
                        spread.getId(),
                        spread.getSymbol(),
                        spread.getDatetime(),
                        spread.getBuyPrice(),
                        spread.getSellPrice(),
                        spread.getType());
            }

            @Override
            public GlobalStreamId getSourceGlobalStreamId() {
                return new GlobalStreamId(LITERAL_QUOTES, Utils.DEFAULT_STREAM_ID);
            }

            @Override
            public String getSourceComponent() {
                return LITERAL_QUOTES;
            }

            @Override
            public int getSourceTask() {
                return 1;
            }

            @Override
            public String getSourceStreamId() {
                return Utils.DEFAULT_STREAM_ID;
            }

            @Override
            public org.apache.storm.tuple.MessageId getMessageId() {
                return null;
            }

            @Override
            public TopologyContext getContext() {
                return null;
            }
        };
    }

    /**
     * Test of execute method, of class EsperBolt in LocalCluster.
     *
     */
    @Test
    public void testEPL() throws Exception {
        tuplesCount.set(0);
        matchedCount.set(0);

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
                        + "having avg(buyPrice) > 0.76")))
                .shuffleGrouping(LITERAL_QUOTES);
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping(LITERAL_ESPER, LITERAL_RETURN_OBJ);

        Config conf = new Config();
        conf.put(Config.STORM_LOCAL_HOSTNAME, "localhost");
        try (LocalCluster cluster = new LocalCluster()) {
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
        }
        long elapsed = Math.max(1L, endMilis - startMilis);
        write("TUPLAS PROCESADAS: " + tuplesCount.get() + " (EMITIDAS: " + matchedCount.get() + ")");
        write("MILISEGUNDOS: " + elapsed);
        write("VELOCIDAD: " + (tuplesCount.get() / elapsed) + " TUPLAS/MILISEGUNDO");
        assertTrue(tuplesCount.get() > 0, "Expected Spout to emit tuples from CSV");
        assertTrue(matchedCount.get() > 0, "Expected EsperBolt to emit matching Result tuples");
    }

    public static class ComparableFileByModified implements Comparable<ComparableFileByModified> {

        public long t;
        public File f;

        public ComparableFileByModified(File file) {
            f = file;
            t = file.lastModified();
        }

        @Override
        public int compareTo(ComparableFileByModified o) {
            long u = o.t;
            return Long.compare(t, u);
        }
    }

    /**
     *
     */
    public static class PrinterBolt extends BaseBasicBolt {

        private static final long serialVersionUID = 1L;

        /**
         *
         * @param tuple
         * @param collector
         */
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            matchedCount.incrementAndGet();
            log("PRICE: " + tuple.getDoubleByField("buyPrice") + " - AVG: " + tuple.getDoubleByField("avg"));
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

        private static final long serialVersionUID = 1L;

        transient SpoutOutputCollector collector;
        transient CellProcessor[] processors;
        transient ICsvBeanReader[] beanReader;
        transient int beanReaderCount;

        /**
         *
         * @param conf
         * @param context
         * @param collector
         */
        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

            this.collector = collector;
            this.processors = createProcessors();
            File[] files = (new File("src/test/resources")).listFiles();
            ComparableFileByModified[] pairs = new ComparableFileByModified[files.length];
            for (int i = 0; i < files.length; i++) {
                pairs[i] = new ComparableFileByModified(files[i]);
            }
            Arrays.sort(pairs);
            this.beanReader = new CsvBeanReader[files.length];
            for (int i = 0; i < pairs.length; i++) {
                try {
                    this.beanReader[i] = new CsvBeanReader(new FileReader(pairs[i].f), CsvPreference.STANDARD_PREFERENCE);
                } catch (FileNotFoundException ex) {
                    Logger.getLogger(EsperBoltStressTest.class.getName()).log(Level.SEVERE, null, ex);
                    this.beanReader[i] = null;
                }
            }
            this.beanReaderCount = 0;
            startMilis = System.currentTimeMillis();
        }

        /**
         *
         */
        @Override
        public void nextTuple() {
            if (beanReaderCount < this.beanReader.length && beanReaderCount < MAX_INPUT_FILES) {
                if (this.beanReader[beanReaderCount] != null) {
                    try {
                        ForexSpreadTestBean spread = beanReader[beanReaderCount].read(
                                ForexSpreadTestBean.class, EsperBoltStressTest.header, processors);
                        if (spread != null) {
                            tuplesCount.incrementAndGet();
                            log(String.format("lineNo=%s, rowNo=%s, spread=%s",
                                    beanReader[beanReaderCount].getLineNumber(),
                                    beanReader[beanReaderCount].getRowNumber(), spread));
                            this.collector.emit(new Values(
                                    spread.getId(),
                                    spread.getSymbol(),
                                    spread.getDatetime(),
                                    spread.getBuyPrice(),
                                    spread.getSellPrice(),
                                    spread.getType()));
                            endMilis = System.currentTimeMillis();
                        } else {
                            beanReader[beanReaderCount].close();
                            beanReader[beanReaderCount] = null;
                            beanReaderCount++;
                        }
                    } catch (IOException ex) {
                        Logger.getLogger(EsperBoltStressTest.class.getName()).log(Level.SEVERE, null, ex);
                        beanReaderCount++;
                    }
                } else {
                    beanReaderCount++;
                }
            } else {
                Utils.sleep(10);
            }
        }

        @Override
        public void close() {
            if (this.beanReader != null) {
                for (int i = 0; i < this.beanReader.length; i++) {
                    if (this.beanReader[i] != null) {
                        try {
                            this.beanReader[i].close();
                        } catch (IOException ignored) {
                        }
                        this.beanReader[i] = null;
                    }
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
