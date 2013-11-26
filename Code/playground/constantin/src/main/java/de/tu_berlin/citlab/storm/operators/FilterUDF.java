package de.tu_berlin.citlab.storm.operators;

import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.ISerializiableExecutable;

public interface FilterUDF extends ISerializiableExecutable<Values, Boolean> {

}
