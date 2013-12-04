package de.tu_berlin.citlab.storm.operators;

import java.util.List;

import de.tu_berlin.citlab.storm.udf.ISerializiableExecutable;

public interface FilterUDF extends
		ISerializiableExecutable<List<Object>, Boolean> {

}
