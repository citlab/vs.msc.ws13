package de.tu_berlin.citlab.storm.operators.join;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.exceptions.InvalidJoinPairCountException;
import de.tu_berlin.citlab.storm.exceptions.JoinException;
import de.tu_berlin.citlab.storm.exceptions.JoinSourceNotFoundException;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.TupleComparator;
import de.tu_berlin.citlab.storm.window.WindowContainer;

public class JoinOperator extends IOperator {
	
	private static final long serialVersionUID = -1921795142772743781L;

	protected JoinUDF joinUDF;
	
	private TupleComparator joinPredicate;
	
	private TupleProjection projection;

	private String innerSource;
	
	private String outerSource;
		
	private Map<String, Queue<WindowContainer<Tuple>>> activeWindows = new HashMap<String, Queue<WindowContainer<Tuple>>> ();

	public JoinOperator(JoinUDF join, TupleComparator joinPredicate, TupleProjection projection, String outer, String inner ) {
		this.joinUDF = join;
		this.joinPredicate = joinPredicate;
		this.innerSource = inner;
		this.outerSource = outer;
		this.projection = projection;
		prepare();
	}


	private void prepare(){
		activeWindows.put(innerSource, new LinkedList<WindowContainer<Tuple>>() );
		activeWindows.put(outerSource, new LinkedList<WindowContainer<Tuple>>() );
		
	}

	public void execute(List<Tuple> tuples, OutputCollector collector ) {
		try {
			String source="";
			if(tuples.size() > 0 ) source = tuples.get(0).getSourceComponent();
			
			if( activeWindows.containsKey(source) ){
				Queue<WindowContainer<Tuple>> windows = activeWindows.get(source);
				windows.add(new WindowContainer<Tuple>(tuples));				
			}
			else {
			}
			
			// find join pairs
			JoinPair pair = getJoinPairs();
			
			// pairs found?
			if(pair != null ){
				// join strategy
				joinUDF.executeJoin(pair, joinPredicate, projection, collector );
			}
			else {
			}
		}catch(JoinException e) {
			System.out.println("error");
		}
	}

	public JoinPair getJoinPairs() throws JoinException {
		if(activeWindows.keySet().size() > 2 ){
			throw new InvalidJoinPairCountException();
		} else {
			Queue<WindowContainer<Tuple>> innerList=activeWindows.get(innerSource);
			Queue<WindowContainer<Tuple>> outerList=activeWindows.get(outerSource);
			
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
