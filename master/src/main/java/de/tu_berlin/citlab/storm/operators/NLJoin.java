package de.tu_berlin.citlab.storm.operators;

import java.util.List;

import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.Context;
import de.tu_berlin.citlab.storm.window.WindowContainer;

public class NLJoin implements JoinUDF {

	/**
	 * 
	 */
	private static final long serialVersionUID = -318981036829194511L;


	@Override
	public WindowContainer<Values> execute(List<WindowContainer<Values>> param, Context context) {
		
		return null;
	}

}
