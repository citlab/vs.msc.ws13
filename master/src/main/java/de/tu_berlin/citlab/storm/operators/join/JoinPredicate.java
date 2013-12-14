package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;

import de.tu_berlin.citlab.storm.window.DataTuple;

public interface JoinPredicate extends Serializable  {
	public boolean evaluate(DataTuple t1, DataTuple t2);
}
