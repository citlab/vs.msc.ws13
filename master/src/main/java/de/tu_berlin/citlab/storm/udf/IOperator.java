package de.tu_berlin.citlab.storm.udf;

import java.util.List;

import backtype.storm.tuple.Values;

public interface IOperator extends
		ISerializiableExecutable<List<Values>, List<Values>> {

}
