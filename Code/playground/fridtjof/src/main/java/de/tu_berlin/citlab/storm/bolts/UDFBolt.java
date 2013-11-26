package de.tu_berlin.citlab.storm.bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.helpers.TupleHelper;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.Window;

public class UDFBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2108744990748224929L;

	protected OutputCollector collector;

	protected Fields inputFields;

	protected Fields outputFields;

	protected IOperator operator;

	protected int windowSize;

	protected int windowSlidingOffset;

	/**
	 * the value <code>0</code> disables time based window semantics
	 */
	protected int windowTimeout = 0;

	/**
	 * the value <code>null</code> indicates, that no grouping by a key should
	 * be done, and all tuples are gathered in a global Window instead
	 */
	protected Fields keyFields = null;

	protected List<Object> defaultKey = new ArrayList<Object>();

	protected Map<List<Object>, Window<Tuple>> windows;

	public UDFBolt(Fields inputFields, Fields outputFields, IOperator operator) {
		this(inputFields, outputFields, operator, 1, null);
	}

	public UDFBolt(Fields inputFields, Fields outputFields, IOperator operator,
			int windowSize) {
		this(inputFields, outputFields, operator, windowSize, null);
	}

	public UDFBolt(Fields inputFields, Fields outputFields, IOperator operator,
			int windowSize, Fields keyFields) {
		this.inputFields = inputFields;
		this.outputFields = outputFields;
		this.operator = operator;
		this.keyFields = keyFields;
		this.windowSize = windowSize;
		// currently no sliding
		windowSlidingOffset = windowSize;
		// currently no timeout
		windowTimeout = 0;
		windows = new HashMap<List<Object>, Window<Tuple>>();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outputFields);
	}

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		if(windowTimeout > 0) {
			conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, windowTimeout);
		}
		return conf;
	}

	public void execute(Tuple input) {
		if (TupleHelper.isTickTuple(input)) {
			for (List<Object> key : windows.keySet()) {
				Window<Tuple> window = windows.get(key);
				executeBatch(window.flush());
			}
		} else {
			List<Object> key;
			if (keyFields == null) {
				key = defaultKey;
			} else {
				key = input.select(keyFields);
			}
			if (!windows.containsKey(key)) {
				windows.put(key, new Window<Tuple>(windowSize));
			}
			Window<Tuple> window = windows.get(key);
			window.add(input);
			if (window.isFull()) {
				executeBatch(window.flush());
			}
		}
	}

	private void executeBatch(List<Tuple> window) {
		List<List<Object>> params = new ArrayList<List<Object>>();
		for (Tuple input : window) {
			params.add(input.select(inputFields));
		}
		List<List<Object>> outputValues = operator.execute(params);
		if (outputValues != null) {
			for (List<Object> outputValue : outputValues) {
				collector.emit(outputValue);
			}
		}
		for (Tuple input : window) {
			collector.ack(input);
		}
	}

}
