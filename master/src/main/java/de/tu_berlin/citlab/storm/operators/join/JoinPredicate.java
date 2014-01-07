package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;

public interface JoinPredicate extends Serializable  {
	public boolean evaluate(Tuple t1, Tuple t2);
}
