package de.tu_berlin.citlab.storm.topologies;


import java.io.Serializable;
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
import de.tu_berlin.citlab.storm.operators.join.JoinFactory;
import de.tu_berlin.citlab.storm.operators.join.JoinOperator;
import de.tu_berlin.citlab.storm.operators.join.MergeJoin;
import de.tu_berlin.citlab.storm.operators.join.NestedLoopJoin;
import de.tu_berlin.citlab.storm.operators.join.SimpleHashJoin;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;
import de.tu_berlin.citlab.storm.window.CountWindow;

public class SlidingCountWindowMergeJoinTestTopologyTwoSources {
	
	private static final int windowSize = 10;
	private static final int slidingOffset = 10;
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		class DataSource extends BaseRichSpout {
			
			private static final long serialVersionUID = -7374814904789368773L;
			
			private String[] ids = new String[] { "key1", "key2" , "key3" };
			
			private int currentId = 0;

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
				_collector.emit(new Values(ids[currentId++ % ids.length], _id));
				_id++;
			}

			public void declareOutputFields(OutputFieldsDeclarer declarer) {
				declarer.declare(new Fields("key", "value"));
			}

			@Override
			public Map<String, Object> getComponentConfiguration() {
				return null;
			}
		}		

		
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("s1", new DataSource(), 1);
		builder.setSpout("s2", new DataSource(), 1);
		
		
		TupleProjection projection = new TupleProjection(){
			public Values project(Tuple left, Tuple right) {
				
				return new Values(  left.getValueByField("key"),
									left.getValueByField("value"),
									right.getValueByField("key"),
									right.getValueByField("value")
									
								);
			}
		};
		
		
		builder.setBolt("slide",
			new UDFBolt(
				null, // no outputFields
				new JoinOperator( 
								new SimpleHashJoin(), 
								KeyConfigFactory.compareByFields(new Fields("key")), 
								projection, 
								"s1", "s2" ), 
				new CountWindow<Tuple>(windowSize, slidingOffset),
				KeyConfigFactory.BySource(),
				KeyConfigFactory.compareByFields(new Fields("key"))
			),
		1)	.shuffleGrouping("s1")
			.shuffleGrouping("s2");

		
		Config conf = new Config();
		conf.setDebug(true);

		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sliding-count-window-group-by-test", conf,
				builder.createTopology());
	}
}
