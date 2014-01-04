package de.tu_berlin.citlab.storm.operators;

import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class FilterOperator implements IOperator {

	private static final long serialVersionUID = -1921795142772743781L;

	protected Fields inputFields;
	protected FilterUDF filter;

	public FilterOperator(Fields inputFields, FilterUDF filter) {
		this.inputFields = inputFields;
		this.filter = filter;
	}

	public void execute(List<Tuple> input, OutputCollector emitter ) {
		for(Tuple param : input) {
			if (filter.evaluate( param )) {
				emitter.emit(param.select(inputFields));
			}
		}
	}

}
