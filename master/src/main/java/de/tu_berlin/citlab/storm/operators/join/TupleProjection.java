package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public interface TupleProjection extends Serializable {
	public Values project(Tuple left, Tuple right);
}
