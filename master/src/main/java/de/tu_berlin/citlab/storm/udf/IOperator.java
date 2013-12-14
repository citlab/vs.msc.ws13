package de.tu_berlin.citlab.storm.udf;

import java.util.List;

import de.tu_berlin.citlab.storm.window.DataTuple;

public interface IOperator extends
		ISerializiableExecutable<List<DataTuple>, List<DataTuple>> {

}
