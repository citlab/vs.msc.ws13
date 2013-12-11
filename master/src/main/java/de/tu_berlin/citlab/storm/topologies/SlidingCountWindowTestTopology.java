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
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.udf.Context;

public class SlidingCountWindowTestTopology {

	private static final int windowSize = 3;
	private static final int slidingOffset = 1;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new CounterProducer(), 1);
		builder.setBolt("slide", 
				new UDFBolt(new Fields("value"), null, new IOperator() {

					private int lastId = 0;
					ArrayList<List<Integer>> compare = new ArrayList<List<Integer>>();

					private void prepareCompare() {
						if (lastId == 0) {
							for (int i = 0; i < windowSize; i++) {
								List<Integer> newElem = new ArrayList<Integer>();
								newElem.add(lastId++);
								compare.add(newElem);
							}
						} else {
							for (int i = 0; i < slidingOffset; i++) {
								if ( ! compare.isEmpty()) {
									compare.remove(0);
								}
								List<Integer> newElem = new ArrayList<Integer>();
								newElem.add(lastId++);
								compare.add(newElem);
							}
							for (int i = 0; i < slidingOffset; i++) {
								
							}
						}
					}

					public List<Values> execute(List<Values> param, Context context) {
						
						prepareCompare();
						if (param.isEmpty()) {
							System.out.println("Empty window received");
						} else if (param.size() != windowSize) {
							throw new RuntimeException(
									"window size exceeded: " + param.size() + ". Expected was " + windowSize);
						} else if ( ! compare.equals(param)) {
							throw new RuntimeException(
									"Wrong window received.\nExpected: "
											+ compare + "\nReceived: "
											+ param);
						}
						return null;
					}
				}, new CountWindow<Tuple>(windowSize, slidingOffset)), 1).shuffleGrouping("spout");

		Config conf = new Config();
		conf.setDebug(true);

		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sliding-count-window-test", conf, builder.createTopology());
	}
}
