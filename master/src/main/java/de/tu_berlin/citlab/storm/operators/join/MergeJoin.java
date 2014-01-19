package de.tu_berlin.citlab.storm.operators.join;

import java.util.Iterator;
import java.util.LinkedList;

import de.tu_berlin.citlab.storm.window.TupleComparator;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class MergeJoin implements JoinUDF {
	private static final long serialVersionUID = -318981036829194511L;

	public void executeJoin(JoinPair pair, TupleComparator comparator, TupleProjection projection, OutputCollector collector ) {
		
		Iterator<Tuple> outerTuples = pair.getOuter().getWindow().iterator();
		Iterator<Tuple> innerTuples = pair.getInner().getWindow().iterator();
		LinkedList<Tuple> activeList = new LinkedList<Tuple>(); 

		Tuple currentLeftTuple = outerTuples.next();
		Tuple currentRightTuple = innerTuples.next();

		while (outerTuples.hasNext() && innerTuples.hasNext()) {

			/*
			if(currentLeftTuple)
			
			while (innerTuples.hasNext()) {
				Tuple innerTuple = outerTuples.next();
				
				// find matches
				/*if( predicate.evaluate( outerTuple,innerTuple ) ) {
					
				}//while
			
			}//while	
			*/
		}
	}
}