package de.tu_berlin.citlab.storm.operators;

import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;

@SuppressWarnings("serial")
public class MapOperator extends IOperator {

	protected Mapper mapper;
	protected boolean chaining = false;

	public MapOperator(Mapper mapper) {
		this.mapper = mapper;
	}

	public MapOperator setChainingAndReturnInstance(boolean chaining) {
		this.chaining = chaining;
		return this;
	}

	public void execute(List<Tuple> input, OutputCollector emitter) {
		for (Tuple param : input) {
			List<Object> mapped = mapper.map(param);
			if (chaining) {
				emitter.emit(param, mapped);
			} else {
				emitter.emit(mapped);
			}
		}
	}

}
