package de.tu_berlin.citlab.storm.operators;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;

public abstract class FilterUDF implements Serializable {
    public void prepare () {}
	public abstract Boolean evaluate(Tuple t);
}
