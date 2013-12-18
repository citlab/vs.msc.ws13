package de.tu_berlin.citlab.storm.udf;

import java.util.List;

import backtype.storm.tuple.Tuple;
public interface IOperator extends
		ISerializiableExecutable<List<Tuple>, Void> {

}
