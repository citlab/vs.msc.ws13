package de.tu_berlin.citlab.storm.operators;

import java.io.Serializable;
import java.util.List;

public interface Reducer extends Serializable {
	public List<Object> reduce(List<Object> values1, List<Object> values2);
}
