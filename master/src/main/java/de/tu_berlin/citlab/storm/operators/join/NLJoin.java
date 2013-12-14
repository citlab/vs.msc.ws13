package de.tu_berlin.citlab.storm.operators.join;

import java.util.ArrayList;
import java.util.List;

import de.tu_berlin.citlab.storm.window.DataTuple;
import de.tu_berlin.citlab.storm.window.WindowContainer;

public class NLJoin implements JoinUDF {
	private static final long serialVersionUID = -318981036829194511L;

	public List<DataTuple> executeJoin(JoinPair pair, JoinPredicate predicate, TupleProjection projection ) {
		List<DataTuple> joinedTuples = new ArrayList<DataTuple>();
		
		WindowContainer<DataTuple> innerWindow = pair.getInner();
		WindowContainer<DataTuple> outerWindow = pair.getOuter();
		
		for(int o=0; o < outerWindow.getWindow().size(); o ++ ) {
			for(int i=0; i < innerWindow.getWindow().size(); i ++ ) {
				DataTuple outerTuple=outerWindow.getWindow().get(o);
				DataTuple innerTuple=innerWindow.getWindow().get(i);
				
				// evaluate for join predicate
				if( predicate.evaluate(outerTuple,innerTuple) ){
					joinedTuples.add( projection.project(outerTuple,innerTuple) );
				}
			}//for			
		}//for
		return joinedTuples;
	}
}
