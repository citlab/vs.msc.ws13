package de.tu_berlin.citlab.storm.udf;

import java.util.List;

import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.WindowContainer;

public interface IBatchOperator extends
		ISerializiableExecutable<List<WindowContainer<List<Object>, Tuple, List<Tuple>>>, List<List<Object>>> {

}
