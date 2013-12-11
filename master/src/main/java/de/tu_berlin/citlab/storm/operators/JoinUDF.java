package de.tu_berlin.citlab.storm.operators;

import java.util.List;

import backtype.storm.tuple.Values;
import de.tu_berlin.citlab.storm.udf.ISerializiableExecutable;
import de.tu_berlin.citlab.storm.window.WindowContainer;

public interface JoinUDF extends
		ISerializiableExecutable<List<WindowContainer<Values> >, WindowContainer<Values>> {
}
