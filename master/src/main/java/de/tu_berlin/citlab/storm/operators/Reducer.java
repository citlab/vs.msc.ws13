package de.tu_berlin.citlab.storm.operators;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Tuple;

public interface Reducer<T> extends Serializable {
	public T reduce( T value, Tuple tuple );

}
