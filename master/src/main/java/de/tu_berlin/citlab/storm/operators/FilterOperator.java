package de.tu_berlin.citlab.storm.operators;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.Context;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class FilterOperator implements IOperator {

	private static final long serialVersionUID = -1921795142772743781L;

	protected FilterUDF filter;

	public FilterOperator(FilterUDF filter) {
		this.filter = filter;
	}

	public List<Values> execute(List<Values> param, Context context ) {
		List<Values> result = null;
		if (filter.execute(param.get(0), context )) {
			result = new ArrayList<Values>(1);
			result.add(param.get(0));
		}
		return result;
	}

}
