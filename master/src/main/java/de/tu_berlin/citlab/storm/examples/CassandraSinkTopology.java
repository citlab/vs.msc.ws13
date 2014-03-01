package de.tu_berlin.citlab.storm.examples;

import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
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
import de.tu_berlin.citlab.storm.sinks.DataSink;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.CountWindow;


class ExampleDataSourceBolt extends BaseRichSpout {
	
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

class CassandraSink implements IOperator {
	private static final long serialVersionUID = -6328347404112474790L;
	
	public CassandraSink(){
		// load cassandra connection 
	}
	
	public void execute(List<Tuple> tuples, OutputCollector collector ) {

		// prepare update
		
		// do the update
		System.out.println("okay i store now:" + tuples.size() );

		
		// shutdown cassandra connection or remember
		
		// forward tuples
		for( Tuple t : tuples ) {
			collector.emit( t.getValues() );
		}
	}
}

public class CassandraSinkTopology {
	private static final int windowSize = 4;
	private static final int slidingOffset = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new ExampleDataSourceBolt(), 1);
		
		builder.setBolt("slide",
			new UDFBolt(
				null, 
				new DataSink( new CassandraSink() ),
				new CountWindow<Tuple>(windowSize, slidingOffset),
				KeyConfigFactory.ByFields("key")
			)
		, 1).shuffleGrouping("spout");

				
		Config conf = new Config();
		conf.setDebug(true);

		conf.setMaxTaskParallelism(1);
		conf.setMaxSpoutPending(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sliding-count-window-group-by-test", conf,
				builder.createTopology());
	}
}
