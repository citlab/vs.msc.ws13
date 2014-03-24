package de.tu_berlin.citlab.storm.operators;

import java.util.ArrayList;
import java.util.List;

import de.tu_berlin.citlab.storm.udf.IOperator;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class ReduceOperator<T> extends IOperator {
	
	protected final Reducer<T> reducer;
	protected final T init;
	protected boolean chaining = false;

	public ReduceOperator(Reducer<T> reducer, T init) {
		this.reducer = reducer;
		this.init = init;
	}

	public ReduceOperator setChainingAndReturnInstance(boolean chaining) {
		this.chaining = chaining;
		return this;
	}

	public void execute(List<Tuple> input, OutputCollector emitter) {
		T result = init;
		for (Tuple t : input) {
			result = reducer.reduce(result, t);
		}
		if (chaining) {
			emitter.emit(input, result);
		} else {
			emitter.emit(result);
		}
	}
}
