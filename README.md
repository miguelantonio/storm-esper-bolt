# storm-esper-bolt

Library that integrates [Esper](https://www.espertech.com/esper/) as an [Apache Storm](https://storm.apache.org/) Bolt.


## License

[GPLv2](https://www.gnu.org/licenses/old-licenses/gpl-2.0.txt).


## Description

This library defines a Storm Bolt that makes it possible to execute Esper EPL and SODA queries on Storm streams:

```java
TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("quotes", new RandomSentenceSpout());

// Defining input events for Esper (Map or POJO Class)
Map<String, Object> eventTypes = new HashMap<>();
eventTypes.put("symbol", String.class);
eventTypes.put("price", Integer.class);

// Using the Bolt (output types can be declared via Map or POJO classes)
builder.setBolt("esper", (new EsperBolt())
        .addEventTypes(eventTypes)
        .addOutputTypes(Collections.singletonMap("Result", Arrays.asList("avg", "price")))
        .addStatements(Collections.singleton("insert into Result "
                + "select avg(price) as avg, price from "
                + "quotes_default(symbol='A').win:length(2) "
                + "having avg(price) > 60.0")))
        .shuffleGrouping("quotes");
builder.setBolt("print", new PrinterBolt()).shuffleGrouping("esper", "Result");
```

See [`EsperBoltTest`](src/test/java/com/variacode/cep/storm/esper/EsperBoltTest.java) for SODA query usage.
