package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.TupleComparator;

public class StaticHashJoinOperator implements IOperator {
	
	private static final long serialVersionUID = -1921795142772743781L;

	private TupleComparator joinComparator;
	
	private TupleProjection projection;
	
	private Map<Serializable,  List<Tuple> > hashTable= new Hashtable<Serializable, List<Tuple>>();
	
	
	public StaticHashJoinOperator(TupleComparator comparator, TupleProjection projection, Iterator<Tuple> inMemoryTuples) {
		this.joinComparator = comparator;
		this.projection = projection;
		
		// build one side hash table
		while (inMemoryTuples.hasNext()) {
			Tuple tuple = inMemoryTuples.next();
			if( hashTable.containsKey( comparator.getTupleKey(tuple) ) ){
				List<Tuple> tuples = hashTable.get( comparator.getTupleKey(tuple) );
				tuples.add(tuple);
				
			} else {
				List<Tuple> tuples = new ArrayList<Tuple>();
				tuples.add(tuple);
				hashTable.put( comparator.getTupleKey(tuple), tuples );
			}
		}
	}
	
	public void execute(List<Tuple> tuples, OutputCollector collector ) {
        UDFBolt.LOGGER.info("StaticHashJoinOperator: test");

		// lookup: stream second side through and generate output
		Iterator<Tuple> windowTuples = tuples.iterator();
		while (windowTuples.hasNext()) {
			Tuple tuple = windowTuples.next();
			if( hashTable.containsKey( joinComparator.getTupleKey(tuple) ) ){
				
				List<Tuple> memoryTuples = hashTable.get( joinComparator.getTupleKey(tuple) );
				for( Tuple memoryTuple : memoryTuples ){
					collector.emit(projection.project(memoryTuple,tuple));
				}
			}//if
		}//while
	}	
}
