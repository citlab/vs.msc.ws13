package de.tu_berlin.citlab.storm.examples;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.operators.Filter;
import de.tu_berlin.citlab.storm.operators.FilterOperator;
import de.tu_berlin.citlab.storm.operators.FlatMapOperator;
import de.tu_berlin.citlab.storm.operators.FlatMapper;
import de.tu_berlin.citlab.storm.operators.MapOperator;
import de.tu_berlin.citlab.storm.operators.Mapper;
import de.tu_berlin.citlab.storm.operators.ReduceOperator;
import de.tu_berlin.citlab.storm.operators.Reducer;
import de.tu_berlin.citlab.storm.window.CountWindow;
public class UDFTestTopology {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new CounterProducer(), 1);
		builder.setBolt(
			"map",
			new UDFBolt(
				new Fields("key", "value"),
				new MapOperator(
					new Mapper() {
						public List<Object> map(Tuple tuple) {
							String newKey = tuple.getStringByField("key") + " mapped";
							int newValue = myExistingFunction((Integer) tuple.getValues().get(1));
							return new Values(newKey, newValue);
						}
						
						public int myExistingFunction(int param) {
							return param + 10;
						}
					}
				).setChainingAndReturnInstance(true)
			),
			1
		).shuffleGrouping("spout");
		
		
		builder.setBolt(
			"flatmap",
			new UDFBolt(
				new Fields("key", "value"),
				new FlatMapOperator(
					new FlatMapper() {
						public List<List<Object>> flatMap(Tuple tuple) {
							List<List<Object>> result = new ArrayList<List<Object>>();
							
							String inputKey = tuple.getStringByField("key");
							int inputValue = tuple.getIntegerByField("value");
							
							result.add(new Values(inputKey + "flatmapped1", myExistingFunction1(inputValue)));
							result.add(new Values(inputKey + "flatmapped2", myExistingFunction2(inputValue)));
							
							return result;
						}
						
						private int myExistingFunction1(int param) {
							return param *= 10;
						}

						private int myExistingFunction2(int param) {
							return param *= -1;
						}
					}
				).setChaining(true)
			),
		1).shuffleGrouping("map");
		
		
		builder.setBolt(
			"filter",
			new UDFBolt(
				new Fields("key", "value"), // output
				new FilterOperator(
					new Filter() {
                        public Boolean predicate(Tuple param) {
							return (Integer) param.getValueByField("value") > 0;
						}
					}
				).setChainingAndReturnInstance(true)
			),
			1
		).shuffleGrouping("flatmap");
		
		
		builder.setBolt(
			"reducer",
			new UDFBolt(
				new Fields("value"),
				new ReduceOperator(
					new Reducer() {
						public List<Object> reduce(Tuple tuple, List<Object> values) {
							return new Values((Integer) values.get(0) + + tuple.getIntegerByField("value"));
						}
					},
					new Values(0)
				).setChainingAndReturnInstance(true),
				new CountWindow<Tuple>(2)
			),
			1
		).fieldsGrouping("filter", new Fields("value"));

		
		Config conf = new Config();
		conf.setDebug(true);

		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("udf-test", conf, builder.createTopology());
	}

}
