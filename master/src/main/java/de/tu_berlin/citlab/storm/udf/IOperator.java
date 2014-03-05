package de.tu_berlin.citlab.storm.udf;

import java.io.Serializable;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.exceptions.OperatorException;

public interface IOperator extends Serializable {

	public void execute(List<Tuple> input, OutputCollector collector) throws OperatorException;

}
