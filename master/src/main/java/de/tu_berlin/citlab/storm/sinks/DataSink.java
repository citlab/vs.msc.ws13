package de.tu_berlin.citlab.storm.sinks;

import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;

public class DataSink implements IOperator {

	private static final long serialVersionUID = -6563062550998641926L;

	private IOperator op;
	
	public DataSink( IOperator op ){
		this.op = op;
	}
	
	public void execute(List<Tuple> tuples, OutputCollector collector) {
		op.execute( tuples, collector);
	}

}
