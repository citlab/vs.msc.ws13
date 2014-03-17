package de.tu_berlin.citlab.storm.operators;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Tuple;

public interface FlatMapper extends Serializable {
	public List<List<Object>> flatMap(Tuple tuple);
}
