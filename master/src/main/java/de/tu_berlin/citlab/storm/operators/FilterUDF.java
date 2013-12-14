package de.tu_berlin.citlab.storm.operators;

import de.tu_berlin.citlab.storm.udf.ISerializiableExecutable;
import de.tu_berlin.citlab.storm.window.DataTuple;

public interface FilterUDF extends ISerializiableExecutable<DataTuple, Boolean> {

}
