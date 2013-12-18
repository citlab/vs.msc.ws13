package de.tu_berlin.citlab.storm.operators;

import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class FilterOperator implements IOperator {

	private static final long serialVersionUID = -1921795142772743781L;

	protected FilterUDF filter;

	public FilterOperator(FilterUDF filter) {
		this.filter = filter;
	}

	public void execute(List<Tuple> param, OutputCollector emitter ) {
		if (filter.evaluate( param.get(0) )) {
			emitter.emit(param.get(0).getValues());
		}
	}

}
