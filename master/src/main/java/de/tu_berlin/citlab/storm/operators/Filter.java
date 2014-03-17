package de.tu_berlin.citlab.storm.operators;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;

public interface Filter extends Serializable {
	public Boolean predicate(Tuple t);
}
