package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;

import de.tu_berlin.citlab.storm.window.TupleComparator;
import backtype.storm.task.OutputCollector;

public interface JoinUDF extends Serializable {
	public void executeJoin(JoinPair pair, TupleComparator joinComparator, TupleProjection projection, OutputCollector collector );
}
