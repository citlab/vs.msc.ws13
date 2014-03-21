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

public class StaticHashJoinOperator extends IOperator {
	
	private static final long serialVersionUID = -1921795142772743781L;

	private TupleComparator joinComparator;
	
	private TupleProjection projection;
	
	private Map<Serializable,  List<Tuple> > hashTable= new Hashtable<Serializable, List<Tuple>>();

    /**
     * init static hash join operator with an empty hashtable
     * @param comparator
     * @param projection
     */
    public StaticHashJoinOperator(TupleComparator comparator, TupleProjection projection ) {
        this.joinComparator = comparator;
        this.projection = projection;
    }

    /**
     * init static hash join operator with a tuple list
     * @param comparator
     * @param projection
     * @param inMemoryTuples
     */
	public StaticHashJoinOperator(TupleComparator comparator, TupleProjection projection, Iterator<Tuple> inMemoryTuples) {
		this.joinComparator = comparator;
		this.projection = projection;

        updateTable(inMemoryTuples);
	}

    /**
     * init static hash join operator providing a predefined hashtable
     *
     * this is interesting if you want update the hashtable outside the operator
     * @param comparator
     * @param projection
     */
    public StaticHashJoinOperator(TupleComparator comparator, TupleProjection projection, Map<Serializable,  List<Tuple> > hashTable ) {
        this.joinComparator = comparator;
        this.projection = projection;

        this.hashTable = hashTable;
    }


    public void buildTable(Iterator<Tuple> inMemoryTuples){
        hashTable.clear();

        // build one side hash table
        while (inMemoryTuples.hasNext()) {
            Tuple tuple = inMemoryTuples.next();
            if( hashTable.containsKey( this.joinComparator.getTupleKey(tuple) ) ){
                List<Tuple> tuples = hashTable.get( this.joinComparator.getTupleKey(tuple) );
                tuples.add(tuple);

            } else {
                List<Tuple> tuples = new ArrayList<Tuple>();
                tuples.add(tuple);
                hashTable.put( this.joinComparator.getTupleKey(tuple), tuples );
            }
        }
    }

    public void updateTable(Iterator<Tuple> inMemoryTuples){
        // build one side hash table
        while (inMemoryTuples.hasNext()) {
            Tuple tuple = inMemoryTuples.next();
            if( hashTable.containsKey( this.joinComparator.getTupleKey(tuple) ) ){
                List<Tuple> tuples = hashTable.get( this.joinComparator.getTupleKey(tuple) );
                tuples.add(tuple);

            } else {
                List<Tuple> tuples = new ArrayList<Tuple>();
                tuples.add(tuple);
                hashTable.put( this.joinComparator.getTupleKey(tuple), tuples );
            }
        }
    }
	
	public void execute(List<Tuple> tuples, OutputCollector collector ) {

		// lookup: stream second side through and generate output
		Iterator<Tuple> windowTuples = tuples.iterator();
		while (windowTuples.hasNext()) {
			Tuple tuple = windowTuples.next();


            if( hashTable.containsKey( joinComparator.getTupleKey(tuple) ) ){
				
				List<Tuple> memoryTuples = hashTable.get( joinComparator.getTupleKey(tuple) );
				for( Tuple memoryTuple : memoryTuples ){
					collector.emit(projection.project(memoryTuple,tuple));
				}

                this.getUDFBolt().log_debug("static join ("+joinComparator.getTupleKey(tuple)+") YES "+tuple );
			}//if
            else {
                this.getUDFBolt().log_debug("static join ("+joinComparator.getTupleKey(tuple)+") NO "+tuple );
            }
		}//while
	}	
}
