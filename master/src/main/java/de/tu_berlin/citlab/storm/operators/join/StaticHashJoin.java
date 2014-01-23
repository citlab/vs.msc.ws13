package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.TupleComparator;


public class StaticHashJoin implements JoinUDF {
	private static final long serialVersionUID = -318981036829194511L;
	
	private Map<Serializable,  List<Tuple> > hashTable = null;
	
	public StaticHashJoin( final Map<Serializable,  List<Tuple> > hashTable){
		this.hashTable = hashTable;
	}
	
	/*
	public void executeJoin(JoinPair pair, TupleComparator comparator, TupleProjection projection, OutputCollector collector ) {
		Iterator<Tuple> outerTuples = pair.getOuter().getWindow().iterator();
		
		// lookup: stream second side through and generate output
		Iterator<Tuple> innerTuples = pair.getInner().getWindow().iterator();
		while (innerTuples.hasNext()) {
			Tuple innerTuple = innerTuples.next();
			if( hashTable.containsKey( comparator.getTupleKey(innerTuple) ) ){
				
				List<Tuple> tuples = hashTable.get( comparator.getTupleKey(innerTuple) );
				for( Tuple t : tuples ){
					collector.emit(projection.project(t,innerTuple));
				}
			}//if
		}//while
	}*/
}