package de.tu_berlin.citlab.storm.operators.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import de.tu_berlin.citlab.storm.exceptions.InvalidJoinPairCountException;
import de.tu_berlin.citlab.storm.exceptions.JoinException;
import de.tu_berlin.citlab.storm.exceptions.JoinSourceNotFoundException;
import de.tu_berlin.citlab.storm.udf.Context;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.DataTuple;
import de.tu_berlin.citlab.storm.window.WindowContainer;

public class JoinOperator implements IOperator {
	public static List<DataTuple> EMPTY_OUTPUT = new ArrayList<DataTuple>();

	private static final long serialVersionUID = -1921795142772743781L;

	protected JoinUDF joinUDF;
	
	private JoinPredicate joinPredicate;
	
	private TupleProjection projection;

	private String innerSource;
	
	private String outerSource;
		
	HashMap<String, Queue<WindowContainer<DataTuple>>> activeWindows = new HashMap<String, Queue<WindowContainer<DataTuple>>> ();

	public JoinOperator(JoinUDF join, JoinPredicate predicate, TupleProjection projection, String inner, String outer ) {
		this.joinUDF = join;
		this.joinPredicate = predicate;
		this.innerSource = inner;
		this.outerSource = outer;
		this.projection = projection;
		prepare();
	}
	

	private void prepare(){
		activeWindows.put(innerSource, new LinkedList<WindowContainer<DataTuple>>() );
		activeWindows.put(outerSource, new LinkedList<WindowContainer<DataTuple>>() );
		
	}

	public List<DataTuple> execute(List<DataTuple> tuples, Context context ) {
		List<DataTuple> joinedTuples = null;;
		try {
			String source=context.getSource();
			
			if( activeWindows.containsKey(source) ){
				Queue<WindowContainer<DataTuple>> windows = activeWindows.get(source);
				windows.add(new WindowContainer<DataTuple>(tuples));				
			}
			else {
				return null;
			}
			
			// find join pairs
			JoinPair pair = getJoinPairs();
			
			// pairs found?
			if(pair != null ){
				// join strategy
				joinedTuples = joinUDF.executeJoin(pair, joinPredicate, projection );
				System.out.println("created tuple pairs "+joinedTuples.size() );
				return joinedTuples;
			}
			else {
				return EMPTY_OUTPUT;
			}
		}catch(JoinException e) {
			System.out.println("error");
			return joinedTuples;
		}
	}

	public JoinPair getJoinPairs() throws JoinException {
		if(activeWindows.keySet().size() > 2 ){
			throw new InvalidJoinPairCountException();
		} else {
			Queue<WindowContainer<DataTuple>> innerList=activeWindows.get(innerSource);
			Queue<WindowContainer<DataTuple>> outerList=activeWindows.get(outerSource);
			
			if( innerList == null || outerList == null ){
				throw new JoinSourceNotFoundException();
			}
			
			if( !innerList.isEmpty() && !outerList.isEmpty() ) {
				return new JoinPair(innerList.poll(), outerList.poll());
			}
			else {
				return null;
			}
			
		}
	}
	
}
