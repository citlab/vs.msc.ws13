package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;
import java.util.List;

import de.tu_berlin.citlab.storm.window.DataTuple;


public interface JoinUDF extends Serializable {
	public List<DataTuple> executeJoin(JoinPair pair, JoinPredicate predicate, TupleProjection projection );
}
