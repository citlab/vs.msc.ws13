package stormFS.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import stormFS.helpers.TupleHelper;
import stormFS.udf.IOperator;
import stormFS.window.CountWindow;
import stormFS.window.TimeWindow;
import stormFS.window.Window;
import stormFS.window.WindowHandler;

public class UDFBoltMock
{
	protected Fields inputFields;

	protected Fields outputFields;

	protected IOperator operator;

	protected WindowHandler windowHandler;

	public UDFBoltMock(Fields inputFields, Fields outputFields, IOperator operator) {
		this(inputFields, outputFields, operator, new CountWindow<Tuple>(1));
	}

	public UDFBoltMock(Fields inputFields, Fields outputFields, IOperator operator,
			Window<Tuple, List<Tuple>> window) {
		this(inputFields, outputFields, operator, window, null);
	}

	public UDFBoltMock(Fields inputFields, Fields outputFields, IOperator operator,
			Window<Tuple, List<Tuple>> window, Fields keyFields) {
		this.inputFields = inputFields;
		this.outputFields = outputFields;
		this.operator = operator;
		windowHandler = new WindowHandler(window, keyFields);
	}

	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		if (windowHandler.getStub() instanceof TimeWindow) {
			conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
					((TimeWindow<Tuple>) windowHandler.getStub()).getTimeSlot());
		}
		return conf;
	}

	public void execute(Tuple input) {
		if (TupleHelper.isTickTuple(input)) {
			executeBatches(windowHandler.flush());
		}
		else {
			windowHandler.add(input);
			if (windowHandler.isSatisfied()) {
				executeBatches(windowHandler.flush());
			}
		}

	}

	private void executeBatches(List<List<Tuple>> windows) {
		for (List<Tuple> window : windows) {
			List<List<Object>> inputValues = new ArrayList<List<Object>>();
			for (Tuple tuple : window) {
				inputValues.add(tuple.select(inputFields));
			}
			List<List<Object>> outputValues = operator.execute(inputValues);
			if (outputValues != null) {
				for (List<Object> outputValue : outputValues) {
					//collector.emit(outputValue);
				}
			}
			for (Tuple tuple : window) {
				//collector.ack(tuple);
			}
		}
	}

}
