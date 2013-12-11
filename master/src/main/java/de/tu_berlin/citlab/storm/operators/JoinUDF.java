package de.tu_berlin.citlab.storm.operators;

import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.ISerializiableExecutable;
import de.tu_berlin.citlab.storm.window.WindowContainer;

public interface JoinUDF extends
		ISerializiableExecutable<List<WindowContainer<Tuple,Fields> >, WindowContainer<Tuple,Fields>> {
}
