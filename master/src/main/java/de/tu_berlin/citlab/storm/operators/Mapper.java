package de.tu_berlin.citlab.storm.operators;

import java.io.Serializable;
import java.util.List;

public interface Mapper extends Serializable {
	public List<Object> map(List<Object> values);
}
