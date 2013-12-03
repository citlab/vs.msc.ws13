package stormFS.operators;

import java.util.ArrayList;
import java.util.List;

import stormFS.udf.IOperator;

public class FilterOperator implements IOperator {

	private static final long serialVersionUID = -1921795142772743781L;

	protected FilterUDF filter;

	public FilterOperator(FilterUDF filter) {
		this.filter = filter;
	}

	public List<List<Object>> execute(List<List<Object>> param) {
		List<List<Object>> result = null;
		if (filter.execute(param.get(0))) {
			result = new ArrayList<List<Object>>(1);
			result.add(param.get(0));
		}
		return result;
	}

}
