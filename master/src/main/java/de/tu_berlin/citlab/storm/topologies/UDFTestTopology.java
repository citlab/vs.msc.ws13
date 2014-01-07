package de.tu_berlin.citlab.storm.topologies;

import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FilterUDF;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import backtype.storm.task.OutputCollector;
public class UDFTestTopology {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new CounterProducer(), 1);
		builder.setBolt(
			"map",
			new UDFBolt(
				new Fields("key", "value"),
				new IOperator() {
					public void execute(List<Tuple> param, OutputCollector collector ) {
						String newKey = param.get(0).getStringByField("key") + " mapped";
						int newValue = myExistingFunction((Integer) param
								.get(0).getValues().get(1));

						collector.emit( new Values(newKey, newValue));

					}
					public int myExistingFunction(int param) {
						return param + 10;
					}
				}
			),
			1
		).shuffleGrouping("spout");
		
		
		builder.setBolt(
			"flatmap",
			new UDFBolt(
				new Fields("key", "value"),
				new IOperator() {
					public void execute(List<Tuple> param, OutputCollector collector ) {
						String inputKey = (String) param.get(0).getValues().get(0);
						int inputValue = (Integer) param.get(0).getValues().get(1);
						collector.emit( new Values(inputKey + "flatmapped1",
								myExistingFunction1(inputValue)) ) ;
						
						collector.emit( new Values(inputKey + "flatmapped2",
								myExistingFunction2(inputValue)));
					}

					private int myExistingFunction1(int param) {
						return param *= 10;
					}

					private int myExistingFunction2(int param) {
						return param *= -1;
					}
				}
			),
		1).shuffleGrouping("map");
		
		
		builder.setBolt(
			"filter",
			new UDFBolt(
				new Fields("value"), // output
				new FilterOperator(
					new Fields("value"), // input
					new FilterUDF() {
						public Boolean evaluate(Tuple param) {
							return (Integer) param.getValueByField("value") > 0;
						}
					}
				)
			),
			1
		).shuffleGrouping("flatmap");
		
		
		builder.setBolt(
			"reducer",
			new UDFBolt(
				new Fields("value"),
				new IOperator() {
					public void execute(List<Tuple> param, OutputCollector collector ) {
						int reduced = 0;
						for (Tuple tupel : param) {
							reduced += (Integer) tupel.getValueByField("value");
						}
						collector.emit( new Values(reduced) );
					}
				},
				new CountWindow<Tuple>(2)
			),
			1
		).fieldsGrouping("filter", new Fields("value"));

		
		Config conf = new Config();
		conf.setDebug(true);

		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("overflow-test", conf, builder.createTopology());
	}

}
