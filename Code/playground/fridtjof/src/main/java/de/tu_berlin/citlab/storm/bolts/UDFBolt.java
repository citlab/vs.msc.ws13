package de.tu_berlin.citlab.storm.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class UDFBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2108744990748224929L;
	
	protected OutputCollector collector;
	
	protected Fields inputFields;
	
	protected Fields outputFields;
	
	protected IOperator operator;
	
	protected List<Tuple> window;
	
	protected int maxCount;
	protected int curCount;
	
	public UDFBolt(Fields inputFields, Fields outputFields, IOperator operator) {
		this(inputFields, outputFields, operator, 1);
	}
	
	public UDFBolt(Fields inputFields, Fields outputFields, IOperator operator, int maxCount) {
		this.inputFields = inputFields;
		this.outputFields = outputFields;
		this.operator = operator;
		this.maxCount = maxCount;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outputFields);
	}

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		window = new ArrayList<Tuple>(maxCount);
		curCount = 0;
	}

	public void execute(Tuple input) {
		window.add(input);
		curCount++;
		if(curCount >= maxCount) {
			executeBatch(window);
			curCount = 0;
			window.clear();
		}
	}

	private void executeBatch(List<Tuple> window) {
		List<List<Object>> params = new ArrayList<List<Object>>();
		for(Tuple input : window) {
			params.add(input.select(inputFields));
		}
		List<List<Object>> outputValues = operator.execute(params);
		if(outputValues != null) {
			for(List<Object> outputValue : outputValues) {
				collector.emit(outputValue);
			}
		}
		for(Tuple input : window) {
			collector.ack(input);
		}
	}
	
}
