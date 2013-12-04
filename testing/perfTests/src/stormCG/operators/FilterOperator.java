package stormCG.operators;

import backtype.storm.tuple.Values;
import stormCG.udf.IOperator;

public class FilterOperator implements IOperator {
	
	private static final long serialVersionUID = 1L;
	
	
/* Global Variables: */
/* ================= */
	
	final protected FilterUDF _filter;
	
	
/* Constructor: */
/* ============ */
	
	public FilterOperator(FilterUDF filter) {
		this._filter = filter;
	}

	
	
/* Public Methods (from IOPerator): */
/* ================================ */
	
	public Values[] execute(Values param) {
		Values[] result = null;
		if(_filter.execute(param)) {
			result = new Values[] { param };
		}
		return result;
	}

}
