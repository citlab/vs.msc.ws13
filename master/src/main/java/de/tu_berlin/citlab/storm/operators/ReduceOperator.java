package de.tu_berlin.citlab.storm.operators;

import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class ReduceOperator {
	
	protected Reducer reducer;
	protected List<Object> init;
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
		List<Object> result = init;
		for (Tuple param : input) {
			result = reducer.reduce(param.getValues(), result);
		}
		if (chaining) {
			emitter.emit(input, result);
		} else {
			emitter.emit(result);
		}
	}
}
