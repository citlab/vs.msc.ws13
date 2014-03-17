package de.tu_berlin.citlab.storm.operators;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Tuple;

public interface Reducer extends Serializable {
	public List<Object> reduce(Tuple tuple, List<Object> values);
}
