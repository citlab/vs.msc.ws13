package de.tu_berlin.citlab.storm.operators;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Tuple;

public interface Mapper extends Serializable {
	public List<Object> map(Tuple tuple);
}
