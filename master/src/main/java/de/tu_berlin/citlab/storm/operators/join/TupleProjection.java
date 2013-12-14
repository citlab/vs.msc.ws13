package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;

import de.tu_berlin.citlab.storm.window.DataTuple;

public interface TupleProjection extends Serializable {
	public DataTuple project(DataTuple left, DataTuple right);
}
