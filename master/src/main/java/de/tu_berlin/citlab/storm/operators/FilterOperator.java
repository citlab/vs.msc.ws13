package de.tu_berlin.citlab.storm.operators;

import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;

@SuppressWarnings("serial")
public class FilterOperator implements IOperator {

	protected Filter filter;
	protected boolean chaining = false;

	public FilterOperator(Filter filter) {
		this.filter = filter;
	}
	
	public FilterOperator setChainingAndReturnInstance(boolean chaining) {
		this.chaining = chaining;
		return this;
	}

	public void execute(List<Tuple> input, OutputCollector emitter ) {
		for(Tuple param : input) {
			if (filter.predicate( param )) {
				if(chaining) {
					emitter.emit(param, param.getValues());
				}
				else {
					emitter.emit(param.getValues());
				}
			}
		}
	}

}
