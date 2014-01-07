package de.tu_berlin.citlab.storm.operators;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;

public interface FilterUDF extends Serializable {
	Boolean evaluate(Tuple t);
}
