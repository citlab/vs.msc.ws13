package de.tu_berlin.citlab.storm.udf;

import java.io.Serializable;

import backtype.storm.task.OutputCollector;

public interface ISerializiableExecutable<I, O> extends Serializable {

	public void execute(I param, OutputCollector collector );
}
