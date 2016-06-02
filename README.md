# storm-esper-bolt

Library that integrates [Esper](http://esper.codehaus.org) as a [Storm](https://github.com/nathanmarz/storm) Bolt.


## License

[GPLv2](http://www.gnu.org/licenses/old-licenses/gpl-2.0.txt).


## Description

This library defines a Storm Bolt that makes possible to execute Esper EPL and SODA queries on Storm streams:

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("quotes", new RandomSentenceSpout());

	//Defining input events for Esper
    Map<String, Object> eventTypes = new HashMap<>();
	    eventTypes.put("symbol", String.class);
	    eventTypes.put("price", Integer.class);

	//Using the Bolt
    builder.setBolt("esper", (new EsperBolt())
            .addEventTypes(eventTypes)
            .addOutputTypes(Collections.singletonMap("Result", Arrays.asList("avg", "price")))
            .addStatements(Collections.singleton("insert into Result "
                            + "select avg(price) as avg, price from "
                            + "quotes_default(symbol='A').win:length(2) "
                            + "having avg(price) > 60.0")))
            .shuffleGrouping("quotes");
    builder.setBolt("print", new PrinterBolt()).shuffleGrouping("esper", "Result");

I recommend seeing the Test for the SODA query.
