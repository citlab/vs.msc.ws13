package de.tu_berlin.citlab.storm.operators;

import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;

@SuppressWarnings("serial")
public class FlatMapOperator extends IOperator {

	protected FlatMapper flatMapper;
	protected boolean chaining = false;

	public FlatMapOperator(FlatMapper flatMapper) {
		this.flatMapper = flatMapper;
	}

	public FlatMapOperator setChaining(boolean chaining) {
		this.chaining = chaining;
		return this;
	}

	public void execute(List<Tuple> input, OutputCollector emitter) {
		for (Tuple param : input) {
			List<List<Object>> flatMapped = flatMapper.flatMap(param);
			for(List<Object> values : flatMapped) {
				if (chaining) {
					emitter.emit(param, values);
				} else {
					emitter.emit(values);
				}
			}

		}
	}

}
