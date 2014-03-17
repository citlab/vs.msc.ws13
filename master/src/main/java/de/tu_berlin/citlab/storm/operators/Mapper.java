package de.tu_berlin.citlab.storm.operators;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("serial")
public abstract class Mapper implements Serializable {
	public abstract List<Object> map(List<Object> values);
}
