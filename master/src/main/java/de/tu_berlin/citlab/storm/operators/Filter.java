package de.tu_berlin.citlab.storm.operators;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public abstract class Filter implements Serializable {
	public abstract Boolean predicate(Tuple t);
}
