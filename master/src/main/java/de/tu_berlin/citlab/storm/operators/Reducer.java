package de.tu_berlin.citlab.storm.operators;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("serial")
public abstract class Reducer implements Serializable {
	public abstract List<Object> reduce(List<Object> values1, List<Object> values2);
}
