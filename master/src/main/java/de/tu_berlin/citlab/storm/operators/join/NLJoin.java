package de.tu_berlin.citlab.storm.operators.join;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.WindowContainer;

public class NLJoin implements JoinUDF {
	private static final long serialVersionUID = -318981036829194511L;

	public void executeJoin(JoinPair pair, JoinPredicate predicate, TupleProjection projection, OutputCollector collector ) {
		
		WindowContainer<Tuple> innerWindow = pair.getInner();
		WindowContainer<Tuple> outerWindow = pair.getOuter();
		
		for(int o=0; o < outerWindow.getWindow().size(); o ++ ) {
			for(int i=0; i < innerWindow.getWindow().size(); i ++ ) {
				Tuple outerTuple=outerWindow.getWindow().get(o);
				Tuple innerTuple=innerWindow.getWindow().get(i);
				
				// evaluate for join predicate
				if( predicate.evaluate(outerTuple,innerTuple) ){
					collector.emit(projection.project(outerTuple,innerTuple));
				}
			}//for			
		}//for
	}
}
