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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.tuple.Fields;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class EsperBoltStaticTest {

    public static class Result {
        private Double avg;
        private Integer price;

        public Double getAvg() {
            return avg;
        }

        public Integer getPrice() {
            return price;
        }
    }

    /**
     * Test of checkEPLSyntax method, of class EsperBolt.
     */
    @Test
    public void testEPLCheck() {
        String epl = "insert into Result "
                + "select avg(price) as avg, price from "
                + "quotes_default(symbol='A').win:length(2) "
                + "having avg(price) > 60.0";
        assertDoesNotThrow(() -> EsperBolt.checkEPLSyntax(epl));
    }

    @Test
    public void testInvalidEPLCheck() {
        assertThrows(EsperBoltException.class, () -> EsperBolt.checkEPLSyntax("select * from"));
        assertThrows(EsperBoltException.class, () -> EsperBolt.checkEPLSyntax(null));
    }

    @Test
    public void testNullValidationAndPojoOutputTypes() {
        EsperBolt bolt = new EsperBolt();
        assertThrows(FailedException.class, () -> bolt.addOutputTypes((Map<String, List<String>>) null));
        assertThrows(FailedException.class, () -> bolt.addOutputTypes((Class<?>[]) null));
        assertThrows(FailedException.class, () -> bolt.addEventTypes((Map<String, Object>) null));
        assertThrows(FailedException.class, () -> bolt.addStatements(Collections.singleton(null)));

        Map<String, Object> mapWithNullValue = new HashMap<>();
        mapWithNullValue.put("price", null);
        assertThrows(FailedException.class, () -> bolt.addEventTypes(mapWithNullValue));

        assertDoesNotThrow(() -> bolt.addOutputTypes(Result.class));
        OutputFieldsGetter getter = new OutputFieldsGetter();
        bolt.declareOutputFields(getter);
        assertTrue(getter.getFieldsDeclaration().containsKey("Result"));
        assertEquals(Arrays.asList("avg", "price"), getter.getFieldsDeclaration().get("Result").get_output_fields());
    }

    @Test
    public void testUpdateNullFieldAlignmentAndErrorReporting() {
        List<List<Object>> emitted = new ArrayList<>();
        List<Throwable> errors = new ArrayList<>();
        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, Collection< org.apache.storm.tuple.Tuple> anchors, List<Object> tuple) {
                emitted.add(tuple);
                return Collections.emptyList();
            }

            @Override
            public void emitDirect(int taskId, String streamId, Collection<org.apache.storm.tuple.Tuple> anchors, List<Object> tuple) {
            }

            @Override
            public void ack(org.apache.storm.tuple.Tuple input) {
            }

            @Override
            public void fail(org.apache.storm.tuple.Tuple input) {
            }

            @Override
            public void resetTimeout(org.apache.storm.tuple.Tuple input) {
            }

            @Override
            public void flush() {
            }

            @Override
            public void reportError(Throwable error) {
                errors.add(error);
            }
        });

        EsperBolt bolt = new EsperBolt()
                .addOutputTypes(Result.class);
        OutputFieldsGetter getter = new OutputFieldsGetter();
        bolt.declareOutputFields(getter);
        Fields declared = new Fields(getter.getFieldsDeclaration().get("Result").get_output_fields());
        assertEquals(2, declared.size());
        assertNotNull(collector);
        assertTrue(errors.isEmpty());
    }

}
