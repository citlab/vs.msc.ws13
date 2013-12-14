package de.tu_berlin.citlab.storm.operators;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.Context;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.DataTuple;

public class FilterOperator implements IOperator {

	private static final long serialVersionUID = -1921795142772743781L;

	protected FilterUDF filter;

	public FilterOperator(FilterUDF filter) {
		this.filter = filter;
	}

	public List<DataTuple> execute(List<DataTuple> param, Context context ) {
		List<DataTuple> result = null;
		if (filter.execute(param.get(0), context )) {
			result = new ArrayList<DataTuple>(1);
			result.add(param.get(0));
		}
		return result;
	}

}
