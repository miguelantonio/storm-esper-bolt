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

public class EsperBoltException extends Exception {

    public EsperBoltException(String msg, Throwable err) {
        super(msg, err);
    }
}
