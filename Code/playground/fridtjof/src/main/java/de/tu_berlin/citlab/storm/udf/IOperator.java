package de.tu_berlin.citlab.storm.udf;

import backtype.storm.tuple.Values;

public interface IOperator extends ISerializiableExecutable<Values, Values[]> {
	
}
