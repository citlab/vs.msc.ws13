package de.tu_berlin.citlab.storm.operators.join;

import java.util.Iterator;

import de.tu_berlin.citlab.storm.window.TupleComparator;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class NestedLoopJoin implements JoinUDF {
	private static final long serialVersionUID = -318981036829194511L;

	public void executeJoin(JoinPair pair, TupleComparator comparator, TupleProjection projection, OutputCollector collector ) {
		Iterator<Tuple> outerTuples = pair.getOuter().getWindow().iterator();
		while (outerTuples.hasNext()) {
			Tuple outerTuple = outerTuples.next();
			
			Iterator<Tuple> innerTuples = pair.getInner().getWindow().iterator();
			while (innerTuples.hasNext()) {
				Tuple innerTuple = innerTuples.next();
				
				// evaluate for join predicate
				if( comparator.compare( outerTuple,innerTuple ) == 0 ){
					collector.emit(projection.project(outerTuple,innerTuple));
				}//if
			}//while
		}//while
	}
}