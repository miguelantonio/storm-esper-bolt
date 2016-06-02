/*
 * Copyright 2015 miguel.
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

import java.util.Date;

public class ForexSpreadTestBean {

    private String id;
    private String symbol;
    private Date datetime;
    private double buyPrice;
    private double sellPrice;
    private String type;

    public ForexSpreadTestBean() {
    }

    public ForexSpreadTestBean(String id, String symbol, Date datetime, double buyPrice, double sellPrice, String type) {
        this.id = id;
        this.symbol = symbol;
        this.datetime = datetime;
        this.buyPrice = buyPrice;
        this.sellPrice = sellPrice;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public String getSymbol() {
        return symbol;
    }

    public Date getDatetime() {
        return datetime;
    }

    public double getBuyPrice() {
        return buyPrice;
    }

    public double getSellPrice() {
        return sellPrice;
    }

    public String getType() {
        return type;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public void setDatetime(Date datetime) {
        this.datetime = datetime;
    }

    public void setBuyPrice(double buyPrice) {
        this.buyPrice = buyPrice;
    }

    public void setSellPrice(double sellPrice) {
        this.sellPrice = sellPrice;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "ForexSpreadTestBean{" + "id=" + id + ", symbol=" + symbol + ", datetime=" + datetime + ", buyPrice=" + buyPrice + ", sellPrice=" + sellPrice + ", type=" + type + '}';
    }

}
