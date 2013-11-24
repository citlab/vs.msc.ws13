package de.tu_berlin.citlab.storm.operators;

import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class FilterOperator implements IOperator {
	
	private static final long serialVersionUID = -1921795142772743781L;
	
	protected FilterUDF filter;
	
	public FilterOperator(FilterUDF filter) {
		this.filter = filter;
	}

	public Values[] execute(Values param) {
		Values[] result = null;
		if(filter.execute(param)) {
			result = new Values[] { param };
		}
		return result;
	}

}
