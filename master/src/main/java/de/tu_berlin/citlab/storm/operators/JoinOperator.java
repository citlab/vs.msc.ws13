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
	
	HashMap<String, List<WindowContainer<Values>>> activeWindows = new HashMap<String, List<WindowContainer<Values>>> ();

	public JoinOperator(JoinUDF join, IKeyConfig keyConfig) {
		this.join = join;
		this.keyConfig = keyConfig;
	}

	public List<Values> execute(List<Values> tuples, Context context ) {
		String vstr="";
		for( Values v : tuples ){
			vstr+=v.toString();
		}//for
		
		List<Values> result = new ArrayList<Values>();
		System.out.println("batch-processing: "+context.getSource()+": "+vstr );
		
		
		return result;
	}
	
	
}
