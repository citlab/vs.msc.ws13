package de.tu_berlin.citlab.storm.topologies;

import java.util.ArrayList;
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
import de.tu_berlin.citlab.storm.window.DataTuple;
import de.tu_berlin.citlab.storm.udf.Context;

public class UDFTestTopology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new CounterProducer(), 1);
		builder.setBolt(
				"map",
				new UDFBolt(new Fields("key", "value"), new Fields("key",
						"value"), new IOperator() {
					private static final long serialVersionUID = 1L;

					public List<DataTuple> execute(List<DataTuple> param, Context context) {
						String newKey = param.get(0).getValues().get(0) + " mapped";
						int newValue = myExistingFunction((Integer) param
								.get(0).getValues().get(1));
						List<DataTuple> result = new ArrayList<DataTuple>(
								1);
						result.add(new DataTuple(new Values(newKey, newValue), new Fields("0", "1")) );
						return result;
					}

					public int myExistingFunction(int param) {
						return param + 10;
					}
				}), 1).shuffleGrouping("spout");
		
		
		builder.setBolt(
				"flatmap",
				new UDFBolt(new Fields("key", "value"), new Fields("key",
						"value"), new IOperator() {
					private static final long serialVersionUID = 1L;

					public List<DataTuple> execute(List<DataTuple> param, Context context) {
						List<DataTuple> result = new ArrayList<DataTuple>();
						String inputKey = (String) param.get(0).getValues().get(0);
						int inputValue = (Integer) param.get(0).getValues().get(1);
						result.add(new DataTuple( new Values(inputKey + "flatmapped1",
								myExistingFunction1(inputValue)), new Fields("key","value")) );
						
						result.add(new DataTuple( new Values(inputKey + "flatmapped2",
								myExistingFunction2(inputValue)), new Fields("key", "value") ));
						return result;
					}

					private int myExistingFunction1(int param) {
						return param *= 10;
					}

					private int myExistingFunction2(int param) {
						return param *= -1;
					}
				}), 1).shuffleGrouping("map");
		
		
		builder.setBolt(
				"filter",
				new UDFBolt(new Fields("value"), new Fields("value"),
						new FilterOperator(new FilterUDF() {
							private static final long serialVersionUID = 1L;

							public Boolean execute(DataTuple param, Context context ) {
								return (Integer) param.getValues().get(0) > 0;
							}
						})), 1).shuffleGrouping("flatmap");
		
		
		builder.setBolt(
				"reducer",
				new UDFBolt(new Fields("value"), new Fields("value"),
						new IOperator() {
							private static final long serialVersionUID = 1L;

							public List<DataTuple> execute(
									List<DataTuple> param, Context context) {
								int reduced = 0;
								List<DataTuple> result = new ArrayList<DataTuple>(
										0);
								for (DataTuple tupel : param) {
									reduced += (Integer) tupel.getValues().get(0);
								}
								result.add(new DataTuple( new Values(reduced), new Fields("key", "value")));
								return result;
							}
						}, new CountWindow<Tuple>(2)), 1).fieldsGrouping(
				"filter", new Fields("value"));

		
		Config conf = new Config();
		conf.setDebug(true);

		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("overflow-test", conf, builder.createTopology());
	}

}
