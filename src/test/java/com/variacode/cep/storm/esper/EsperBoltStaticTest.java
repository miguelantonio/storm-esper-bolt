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

import static org.junit.Assert.*;
import org.junit.Test;

public class EsperBoltStaticTest {

    /**
     * Test of checkEPLSyntax method, of class EsperBolt.
     */
    @Test
    public void testEPLCheck() {
        String epl = "insert into Result "
                                + "select avg(price) as avg, price from "
                                + "quotes_default(symbol='A').win:length(2) "
                                + "having avg(price) > 60.0";
        try {
            EsperBolt.checkEPLSyntax(epl);
        } catch (EsperBoltException ex) {
            fail(ex.getMessage());
        }
    }

}
