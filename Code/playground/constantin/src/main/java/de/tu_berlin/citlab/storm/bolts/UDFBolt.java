package de.tu_berlin.citlab.storm.bolts;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class UDFBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 2108744990748224929L;
	
	protected Fields inputFields;
	
	protected Fields outputFields;
	
	protected IOperator operator;
	
	public UDFBolt(Fields inputFields, Fields outputFields, IOperator operator) {
		this.inputFields = inputFields;
		this.outputFields = outputFields;
		this.operator = operator;
	}
	
	public final void execute(Tuple input, BasicOutputCollector collector) {
		Values params = (Values) input.select(inputFields);
		Values[] outputValues = operator.execute(params);
		if(outputValues != null) {
			for(List<Object> outputValue : outputValues) {
				collector.emit(outputValue);
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outputFields);
	}
	
}
