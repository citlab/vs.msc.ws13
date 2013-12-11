package de.tu_berlin.citlab.storm.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.helpers.ValuesHelper;
import de.tu_berlin.citlab.storm.udf.Context;
import de.tu_berlin.citlab.storm.udf.IKeyConfig;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.WindowContainer;

public class JoinOperator implements IOperator {

	private static final long serialVersionUID = -1921795142772743781L;

	protected JoinUDF join;
	
	private IKeyConfig keyConfig;
	
	private Fields keys;
	
	HashMap<Values, List<WindowContainer<Values>>> activeWindows = new HashMap<Values, List<WindowContainer<Values>>> ();

	public JoinOperator(JoinUDF join, IKeyConfig keyConfig) {
		this.join = join;
		this.keyConfig = keyConfig;
	}

	public List<Values> execute(List<Values> tuples, Context context ) {
		// output data
		List<Values> result = null;
		
		return result;
	}
}
