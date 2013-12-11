package de.tu_berlin.citlab.storm.operators;

import java.util.HashMap;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.WindowContainer;

public class JoinOperator implements IOperator {

	private static final long serialVersionUID = -1921795142772743781L;

	protected JoinUDF join;
	
	HashMap<String, WindowContainer<Tuple, Fields>> active_windows = new HashMap<String, WindowContainer<Tuple, Fields>>();

	public JoinOperator(JoinUDF join ) {
		this.join = join;
	}

	public List<List<Object>> execute(List<List<Object>> tuples) {
		List<List<Object>> result = null;

		// okay i am now executed on a window
		WindowContainer<Tuple, Fields> window = new WindowContainer(tuples, new Fields() );
		
		return result;
	}
}
