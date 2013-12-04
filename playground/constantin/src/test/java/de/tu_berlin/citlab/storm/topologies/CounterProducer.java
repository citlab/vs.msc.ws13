package de.tu_berlin.citlab.storm.topologies;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class CounterProducer extends BaseRichSpout {
	
	private static final long serialVersionUID = -7374814904789368773L;
	
	int _id = 0;
	
	SpoutOutputCollector _collector;
	
	@Override
	public void ack(Object msgId) {}
	
	@Override
	public void fail(Object msgId) {}
	
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	public void nextTuple() {
		Utils.sleep(500);
		_collector.emit(new Values("key: " + _id, _id));
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