package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;
import backtype.storm.task.OutputCollector;

public interface JoinUDF extends Serializable {
	public void executeJoin(JoinPair pair, JoinPredicate predicate, TupleProjection projection, OutputCollector collector );
}
