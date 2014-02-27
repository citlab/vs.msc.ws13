package de.tu_berlin.citlab.storm.examples;

import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.helpers.KeyConfigFactory;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;
import backtype.storm.task.OutputCollector;

public class SlidingCountWindowGroupingWithMultiKeyTestTopology {
	private static final int windowSize = 10;
	private static final int slidingOffset = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new BaseRichSpout() {
			
			private static final long serialVersionUID = -7374814904789368773L;
			
			private String[] key1 = new String[] { "key1_1", "key1_2" , "key1_3" };
			
			private String[] key2 = new String[] { "key2_1", "key2_2" };
			
			private int currentKey1Index = 0;
			private int currentKey2Index = 0;

			int _id = 0;

			SpoutOutputCollector _collector;

			@Override
			public void ack(Object msgId) {
			}

			@Override
			public void fail(Object msgId) {
			}

			public void open(@SuppressWarnings("rawtypes") Map conf,
					TopologyContext context, SpoutOutputCollector collector) {
				_collector = collector;
			}

			public void nextTuple() {
				Utils.sleep(500);
				_collector.emit(new Values(key1[currentKey1Index++ % key1.length], key2[currentKey2Index++ % key2.length], _id));
				_id++;
			}

			public void declareOutputFields(OutputFieldsDeclarer declarer) {
				declarer.declare(new Fields("key1", "key2", "value"));
			}

			@Override
			public Map<String, Object> getComponentConfiguration() {
				return null;
			}
		}, 1);
		builder.setBolt("slide",
			new UDFBolt(
				null, // no outputFields
				new IOperator() {
					public void execute(List<Tuple> param, OutputCollector collector ) {
						System.out.println(param);
					}
				},
				new CountWindow<Tuple>(windowSize, slidingOffset),
				KeyConfigFactory.ByFields("key1", "key2")
			),
		1).shuffleGrouping("spout");

		Config conf = new Config();
		conf.setDebug(true);

		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sliding-count-window-group-by-test", conf,
				builder.createTopology());
	}
}
