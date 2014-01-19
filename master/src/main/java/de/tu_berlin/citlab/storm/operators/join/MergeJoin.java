package de.tu_berlin.citlab.storm.operators.join;

import java.util.Iterator;
import java.util.LinkedList;

import de.tu_berlin.citlab.storm.window.TupleComparator;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class MergeJoin implements JoinUDF {
	private static final long serialVersionUID = -318981036829194511L;

	public void executeJoin(JoinPair pair, TupleComparator tupleComparator, TupleProjection projection, OutputCollector collector ) {
		
		Iterator<Tuple> outerTuples = pair.getOuter().getWindow().iterator();
		Iterator<Tuple> innerTuples = pair.getInner().getWindow().iterator();
		LinkedList<Tuple> activeList = new LinkedList<Tuple>(); 

		Tuple currentOuterTuple = outerTuples.next();
		Tuple currentInnerTuple = innerTuples.next();

		while (outerTuples.hasNext() && innerTuples.hasNext()) {

			if( tupleComparator.compare( currentOuterTuple, currentInnerTuple) < 0  ){
				outerTuples.next();
			}
			else if( tupleComparator.compare( currentOuterTuple, currentInnerTuple) > 0 ){
				innerTuples.next();
			}
			else {	
				/* keys are the same */
				activeList.clear();
				Tuple keyTuple = currentOuterTuple;
				
				// find matching tuples from inner and store the matched list separately
				while(innerTuples.hasNext() && tupleComparator.compare( keyTuple, currentInnerTuple) == 0 ){
					activeList.add( currentInnerTuple );
					currentInnerTuple = innerTuples.next();
				}
				
				// for each matching on the outer side emit a tuple with all matched inner side
				while(outerTuples.hasNext() && tupleComparator.compare( currentOuterTuple, keyTuple) == 0 ){
					for( Tuple e : activeList ){
						collector.emit(	projection.project(currentOuterTuple,e)	);
					}//for
					currentOuterTuple = outerTuples.next();
				} //while
			} //if
			
		} // while
		
	}
}