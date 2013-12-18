package de.tu_berlin.citlab.storm.topologies;


import java.util.ArrayList;
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
import de.tu_berlin.citlab.storm.operators.join.JoinOperator;
import de.tu_berlin.citlab.storm.operators.join.JoinPredicate;
import de.tu_berlin.citlab.storm.operators.join.NLJoin;
import de.tu_berlin.citlab.storm.operators.join.TupleProjection;
import de.tu_berlin.citlab.storm.udf.IKeyConfig;
import de.tu_berlin.citlab.storm.window.CountWindow;
import de.tu_berlin.citlab.storm.window.DataTuple;
import de.tu_berlin.citlab.storm.window.TimeWindow;

class DataSource1 extends BaseRichSpout {
	
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
		Utils.sleep(1);
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

public class SlidingTimeWindowJoinTestTopologyTwoSources {
	private static final int windowSize = 1000;
	private static final int slidingOffset = 500;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("s1", new DataSource1(), 1);
		builder.setSpout("s2", new DataSource1(), 1);
		
		
		
		IKeyConfig groupKey = new IKeyConfig(){
			public List<Object> sortWithKey( Tuple tuple, Fields keyFields) {
				List<Object> key=new ArrayList<Object>();
				key.add( tuple.getSourceComponent() );
				return key;
			}
		};
		
		JoinPredicate joinPredicate = new JoinPredicate() {
			@Override
			public boolean evaluate(DataTuple t1, DataTuple t2) {
				return ((String)t1.get("key")).compareTo( (String)t2.get("key") ) == 0;
			}
		};
		
		
		TupleProjection projection = new TupleProjection(){
			@Override
			public DataTuple project(DataTuple left, DataTuple right) {
				DataTuple out = new DataTuple();
				out.set("key", left.get("key"));
				out.set("value", left.get("value") );
				out.set("keyR", right.get("key") );
				out.set("valueR", right.get("value") );
				return out;
			}
		};
		
		builder.setBolt("slide",
				new UDFBolt(new Fields("key", "value"), null, new JoinOperator( new NLJoin(), joinPredicate, projection, "s1", "s2" ), 
				new TimeWindow<Tuple>(windowSize, slidingOffset), new Fields("key"), groupKey), 1)
				.shuffleGrouping("s1")
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
