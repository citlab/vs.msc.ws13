package de.tu_berlin.citlab.storm.operators;

import java.util.ArrayList;
import java.util.List;

import de.tu_berlin.citlab.storm.udf.IOperator;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class ReduceOperator implements IOperator {
	
	protected final Reducer reducer;
	protected final List<Object> init;
	protected boolean chaining = false;

	public ReduceOperator(Reducer reducer, List<Object> init) {
		this.reducer = reducer;
		this.init = init;
	}

	public ReduceOperator setChaining(boolean chaining) {
		this.chaining = chaining;
		return this;
	}

	public void execute(List<Tuple> input, OutputCollector emitter) {
		List<Object> result = new ArrayList<Object>(init);
		for (Tuple param : input) {
			result = reducer.reduce(param, result);
		}
		if (chaining) {
			emitter.emit(input, result);
		} else {
			emitter.emit(result);
		}
	}
}
